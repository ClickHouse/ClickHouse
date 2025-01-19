---
sidebar_label: Amazon Glue
sidebar_position: 1
slug: /ja/integrations/glue
description: ClickHouseとAmazon Glueの統合
keywords: [ clickhouse, amazon, aws, glue, データ移行, data ]
---

# ClickHouseとAmazon Glueの統合

[Amazon Glue](https://aws.amazon.com/glue/)は、Amazon Web Services (AWS)が提供する完全に管理されたサーバーレスのデータ統合サービスです。これは、分析、機械学習、アプリケーション開発のためのデータの発見、準備、および変換のプロセスを簡素化します。

現在のところ、Glue用のClickHouseコネクタは利用できませんが、公式のJDBCコネクタを利用してClickHouseと接続および統合を行うことができます:

```java
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._
import com.amazonaws.services.glue.log.GlueLogger


// Glueジョブの初期化
object GlueJob {
  def main(sysArgs: Array[String]) {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession
    val logger = new GlueLogger
     import spark.implicits._
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // JDBC接続の詳細
    val jdbcUrl = "jdbc:ch://{host}:{port}/{schema}"
    val jdbcProperties = new java.util.Properties()
    jdbcProperties.put("user", "default")
    jdbcProperties.put("password", "*******")
    jdbcProperties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")

    // ClickHouseからテーブルをロード
    val df: DataFrame = spark.read.jdbc(jdbcUrl, "my_table", jdbcProperties)

    // Spark dfを表示、もしくは他の用途に使用
    df.show()

    // ジョブをコミット
    Job.commit()
  }
}
```

詳細については、[Spark & JDBC ドキュメント](/ja/integrations/apache-spark#read-data)をご覧ください。
