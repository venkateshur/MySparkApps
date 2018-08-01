package com.spark.Apps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.Trigger

case class Person(Name:String,City:String,Country:String,Alive:String)
object ReadStream {

  def main(args: Array[String]) {

    val inPath = args(0)
    val outPath = args(1)
    val checkPointPath = args(2)
    System.setProperty("hadoop.home.dir", "C:/winutils")
    val spark = SparkSession.builder().master("local[*]").appName("ReadStream").getOrCreate()
   //             spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

    // Explicit schema with nullables false
    val mySchema = Encoders.product[Person].schema
    val in = spark.readStream
      .schema(mySchema)
      .format("csv")
      .option("inferSchema", false)
      .option("header", true)
      .option("maxFilesPerTrigger", 1)
      .load(inPath)

    println("Is the query streaming" +  in.isStreaming)
    println("Are there any streaming queries?" +  spark.streams.active.isEmpty)
    val show1 = in.select("*")

    val out = in.
      writeStream.
     // format("csv").
      format("console").
      option("checkpointLocation", checkPointPath).
      option("truncate", false).
     // option("path", outPath).
      trigger(Trigger.ProcessingTime("5 seconds")).
      queryName("readFileStream").
      outputMode("append").
      start


   // out.awaitTermination()
    out.stop()

  }
}