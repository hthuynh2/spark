/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

import scala.math.random

/** Computes an approximation to pi */
object WordCount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Word Count")
      .getOrCreate()
    val inputName = if (args.length > 0) args(0).toString else ""
//    wordCountFunc1(spark, inputName)
//    wordCountFunc2(spark)
//    wordCountFunc3(spark)
//    wordCountFunc4(spark, inputName)
    testAccum(spark)
    spark.stop()
  }

  def wordCountFunc1(spark: SparkSession, inputName: String): Unit = {
    val lines = spark.sparkContext.textFile(inputName)
    val words = lines.flatMap(_.split(" ").map(w => (w,1)))
    val distinctWord = words.groupByKey().groupBy(w => w._2)
    println("distinctWord.todeBugString1: ")
    println(distinctWord.toDebugString)

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd1")))
      fs.delete(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd1"), true)
    distinctWord.saveAsTextFile("/user/hhuynh02/asd1")
  }

  def wordCountFunc2(spark: SparkSession): Unit = {
    val lines = spark.sparkContext.parallelize(List("THIS IS A SHORT SENTECE", "Another Sentence"), 2)
    val words = lines.flatMap(_.split(" ").map(w => (w,1)))
    val distinctWord = words.groupBy(w => w._1)
    println("distinctWord.todeBugString2: ")
    println(distinctWord.toDebugString)

    val lines1 = spark.sparkContext.parallelize(List("THIS IS A SHORT SENTECE", "Another Sentence"), 2)
    val words1 = lines1.flatMap(_.split(" ").map(w => (w,1)))
    val distinctWord1 = words1.groupBy(w => w._1)

    val myUnionRDD = distinctWord.union(distinctWord1)
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd2")))
      fs.delete(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd2"), true)
    myUnionRDD.saveAsTextFile("/user/hhuynh02/asd2")
  }

  def wordCountFunc3(spark: SparkSession): Unit = {

    val lines = spark.sparkContext.parallelize(List("THIS IS A SHORT SENTECE", "Another Sentence"), 2)
    val words = lines.flatMap(_.split(" ").map(w => (w,1)))
    val distinctWord = words.groupBy(w => w._1)
    println("distinctWord.todeBugString2: ")
    println(distinctWord.toDebugString)

    val lines1 = spark.sparkContext.parallelize(List("THIS IS A SHORT SENTECE", "Another Sentence"), 2)
    val words1 = lines1.flatMap(_.split(" ").map(w => (w,1)))
    val distinctWord1 = words1.groupBy(w => w._1)

    val myRdd = distinctWord.cartesian(distinctWord1)
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd2")))
      fs.delete(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd2"), true)
    myRdd.saveAsTextFile("/user/hhuynh02/asd2")


//    val lines = spark.sparkContext.parallelize(List("THIS IS A SHORT SENTECE", "Another Sentence"), 2)
//    val words = lines.flatMap(_.split(" ").map(w => (w,1)))
//    println("distinctWord.todeBugString3: ")
//    println(words.toDebugString)
//    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
//
//    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd3")))
//      fs.delete(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd3"), true)
//    words.saveAsTextFile("/user/hhuynh02/asd3")
  }

  def wordCountFunc4(spark: SparkSession, inputName: String): Unit = {
    val lines = spark.sparkContext.textFile(inputName)
    val words = lines.flatMap(_.split(" ").map(w => (w,1)))
    val caoalesceRdd = words.coalesce(2, true)

    println("distinctWord.todeBugString4: ")
    println(caoalesceRdd.toDebugString)
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd1")))
      fs.delete(new org.apache.hadoop.fs.Path("/user/hhuynh02/asd1"), true)
    caoalesceRdd.saveAsTextFile("/user/hhuynh02/asd1")
  }

  def testAccum(spark: SparkSession): Unit = {
    val accum = spark.sparkContext.longAccumulator("My Accumulator")
    val temp = spark.sparkContext.parallelize(0 to 1000, 100)

    val my_rdd = temp.map(x => {
      accum.add(x)
      (x,x)
    }).groupByKey()

    println(my_rdd.count)
    println(accum.value)
  }

}
// scalastyle:on println
