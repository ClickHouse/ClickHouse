---
sidebar_label: Apache Spark
sidebar_position: 1
slug: /ja/integrations/apache-spark/
description: ClickHouseとApache Sparkの統合の紹介
keywords: [ clickhouse, apache, spark, migrating, data ]
---

# ClickHouseとApache Sparkの統合

[Apache Spark](https://spark.apache.org/) Apache Spark™は、シングルノードマシンまたはクラスターでデータエンジニアリング、データサイエンス、および機械学習を実行するための多言語エンジンです。

Apache SparkとClickHouseを接続する主な方法は2つあります：

1. [Spark Connector](#spark-connector) - Sparkコネクタは`DataSourceV2`を実装し、独自のカタログ管理を備えています。現在、ClickHouseとSparkを統合する推奨方法です。
2. [Spark JDBC](#spark-jdbc) - [JDBCデータソース](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)を使用してSparkとClickHouseを統合します。

## Spark Connector

このコネクタは、高度なパーティショニングや述語プッシュダウンなど、ClickHouseに特化した最適化を活用してクエリ性能やデータ処理を向上させます。コネクタは[ClickHouseの公式JDBCコネクタ](https://github.com/ClickHouse/clickhouse-java)に基づいており、独自のカタログを管理します。

### 要件

- Java 8または17
- Scala 2.12または2.13
- Apache Spark 3.3または3.4または3.5

### 互換性マトリックス

| バージョン | 互換性のあるSparkバージョン | ClickHouse JDBCバージョン |
|------------|-----------------------------|---------------------------|
| main       | Spark 3.3, 3.4, 3.5         | 0.6.3                     |
| 0.8.0      | Spark 3.3, 3.4, 3.5         | 0.6.3                     |
| 0.7.3      | Spark 3.3, 3.4              | 0.4.6                     |
| 0.6.0      | Spark 3.3                   | 0.3.2-patch11             |
| 0.5.0      | Spark 3.2, 3.3              | 0.3.2-patch11             |
| 0.4.0      | Spark 3.2, 3.3              | 依存なし                  |
| 0.3.0      | Spark 3.2, 3.3              | 依存なし                  |
| 0.2.1      | Spark 3.2                   | 依存なし                  |
| 0.1.2      | Spark 3.2                   | 依存なし                  |

### ライブラリのダウンロード

バイナリJARの名前パターンは：

```
clickhouse-spark-runtime-${spark_binary_version}_${scala_binary_version}-${version}.jar
```

すべてのリリースされたJARは[Maven Central Repository](https://repo1.maven.org/maven2/com/clickhouse/spark/)で見つけることができ、すべてのデイリービルドのSNAPSHOT JARは[Sonatype OSS Snapshots Repository](https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/)で確認できます。

### 依存関係としてのインポート

#### Gradle

```
dependencies {
  implementation("com.clickhouse.spark:clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }}")
  implementation("com.clickhouse:clickhouse-jdbc:{{ clickhouse_jdbc_version }}:all") { transitive = false }
}
```

SNAPSHOTバージョンを使用したい場合は、次のリポジトリを追加します：

```
repositories {
  maven { url = "https://s01.oss.sonatype.org/content/repositories/snapshots" }
}
```

#### Maven

```
<dependency>
  <groupId>com.clickhouse.spark</groupId>
  <artifactId>clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}</artifactId>
  <version>{{ stable_version }}</version>
</dependency>
<dependency>
  <groupId>com.clickhouse</groupId>
  <artifactId>clickhouse-jdbc</artifactId>
  <classifier>all</classifier>
  <version>{{ clickhouse_jdbc_version }}</version>
  <exclusions>
    <exclusion>
      <groupId>*</groupId>
      <artifactId>*</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

SNAPSHOTバージョンを使用したい場合は、次のリポジトリを追加します。

```
<repositories>
  <repository>
    <id>sonatype-oss-snapshots</id>
    <name>Sonatype OSS Snapshots Repository</name>
    <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

## Spark SQLで遊ぶ

注意：SQLのみの使用例に対しては、[Apache Kyuubi](https://github.com/apache/kyuubi)を本番環境で使用することが推奨されます。

### Spark SQL CLIの起動

```shell
$SPARK_HOME/bin/spark-sql \
  --conf spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog \
  --conf spark.sql.catalog.clickhouse.host=${CLICKHOUSE_HOST:-127.0.0.1} \
  --conf spark.sql.catalog.clickhouse.protocol=http \
  --conf spark.sql.catalog.clickhouse.http_port=${CLICKHOUSE_HTTP_PORT:-8123} \
  --conf spark.sql.catalog.clickhouse.user=${CLICKHOUSE_USER:-default} \
  --conf spark.sql.catalog.clickhouse.password=${CLICKHOUSE_PASSWORD:-} \
  --conf spark.sql.catalog.clickhouse.database=default \
  --jars /path/clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }}.jar,/path/clickhouse-jdbc-{{ clickhouse_jdbc_version }}-all.jar
```

次の引数

```
  --jars /path/clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }}.jar,/path/clickhouse-jdbc-{{ clickhouse_jdbc_version }}-all.jar
```

は次のように置き換えることができます

```
  --repositories https://{maven-cental-mirror or private-nexus-repo} \
  --packages com.clickhouse.spark:clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }},com.clickhouse:clickhouse-jdbc:{{ clickhouse_jdbc_version }}:all
```

これにより、JARをSparkクライアントノードにコピーする必要がなくなります。

## 操作

基本的な操作、例えばデータベースの作成、テーブルの作成、テーブルへの書き込み、テーブルの読み込みなど。

```
spark-sql> use clickhouse;
Time taken: 0.016 seconds

spark-sql> create database if not exists test_db;
Time taken: 0.022 seconds

spark-sql> show databases;
default
system
test_db
Time taken: 0.289 seconds, Fetched 3 row(s)

spark-sql> CREATE TABLE test_db.tbl_sql (
         >   create_time TIMESTAMP NOT NULL,
         >   m           INT       NOT NULL COMMENT 'part key',
         >   id          BIGINT    NOT NULL COMMENT 'sort key',
         >   value       STRING
         > ) USING ClickHouse
         > PARTITIONED BY (m)
         > TBLPROPERTIES (
         >   engine = 'MergeTree()',
         >   order_by = 'id',
         >   settings.index_granularity = 8192
         > );
Time taken: 0.242 seconds

spark-sql> insert into test_db.tbl_sql values
         > (timestamp'2021-01-01 10:10:10', 1, 1L, '1'),
         > (timestamp'2022-02-02 10:10:10', 2, 2L, '2')
         > as tabl(create_time, m, id, value);
Time taken: 0.276 seconds

spark-sql> select * from test_db.tbl_sql;
2021-01-01 10:10:10	1	1	1
2022-02-02 10:10:10	2	2	2
Time taken: 0.116 seconds, Fetched 2 row(s)

spark-sql> insert into test_db.tbl_sql select * from test_db.tbl_sql;
Time taken: 1.028 seconds

spark-sql> insert into test_db.tbl_sql select * from test_db.tbl_sql;
Time taken: 0.462 seconds

spark-sql> select count(*) from test_db.tbl_sql;
6
Time taken: 1.421 seconds, Fetched 1 row(s)

spark-sql> select * from test_db.tbl_sql;
2021-01-01 10:10:10	1	1	1
2021-01-01 10:10:10	1	1	1
2021-01-01 10:10:10	1	1	1
2022-02-02 10:10:10	2	2	2
2022-02-02 10:10:10	2	2	2
2022-02-02 10:10:10	2	2	2
Time taken: 0.123 seconds, Fetched 6 row(s)

spark-sql> delete from test_db.tbl_sql where id = 1;
Time taken: 0.129 seconds

spark-sql> select * from test_db.tbl_sql;
2022-02-02 10:10:10	2	2	2
2022-02-02 10:10:10	2	2	2
2022-02-02 10:10:10	2	2	2
Time taken: 0.101 seconds, Fetched 3 row(s)
```

## Spark Shellで遊ぶ

### Spark Shellの起動

```shell
$SPARK_HOME/bin/spark-shell \
  --conf spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog \
  --conf spark.sql.catalog.clickhouse.host=${CLICKHOUSE_HOST:-127.0.0.1} \
  --conf spark.sql.catalog.clickhouse.protocol=http \
  --conf spark.sql.catalog.clickhouse.http_port=${CLICKHOUSE_HTTP_PORT:-8123} \
  --conf spark.sql.catalog.clickhouse.user=${CLICKHOUSE_USER:-default} \
  --conf spark.sql.catalog.clickhouse.password=${CLICKHOUSE_PASSWORD:-} \
  --conf spark.sql.catalog.clickhouse.database=default \
  --jars /path/clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }}.jar,/path/clickhouse-jdbc-{{ clickhouse_jdbc_version }}-all.jar
```

次の引数

```
  --jars /path/clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }}.jar,/path/clickhouse-jdbc-{{ clickhouse_jdbc_version }}-all.jar
```

は次のように置き換えることができます

```
  --repositories https://{maven-cental-mirror or private-nexus-repo} \
  --packages com.clickhouse.spark:clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }},com.clickhouse:clickhouse-jdbc:{{ clickhouse_jdbc_version }}:all
```

これにより、JARをSparkクライアントノードにコピーする必要がなくなります。

### 操作

基本的な操作、例えばデータベースの作成、テーブルの作成、テーブルへの書き込み、テーブルの読み込みなど。

```
scala> spark.sql("use clickhouse")
res0: org.apache.spark.sql.DataFrame = []

scala> spark.sql("create database test_db")
res1: org.apache.spark.sql.DataFrame = []

scala> spark.sql("show databases").show
+---------+
|namespace|
+---------+
|  default|
|   system|
|  test_db|
+---------+

scala> spark.sql("""
     | CREATE TABLE test_db.tbl (
     |   create_time TIMESTAMP NOT NULL,
     |   m           INT       NOT NULL COMMENT 'part key',
     |   id          BIGINT    NOT NULL COMMENT 'sort key',
     |   value       STRING
     | ) USING ClickHouse
     | PARTITIONED BY (m)
     | TBLPROPERTIES (
     |   engine = 'MergeTree()',
     |   order_by = 'id',
     |   settings.index_granularity = 8192
     | )
     | """)
res2: org.apache.spark.sql.DataFrame = []

scala> :paste
// Pasteモードに入ります（ctrl-Dで終了）

spark.createDataFrame(Seq(
    ("2021-01-01 10:10:10", 1L, "1"),
    ("2022-02-02 10:10:10", 2L, "2")
)).toDF("create_time", "id", "value")
    .withColumn("create_time", to_timestamp($"create_time"))
    .withColumn("m", month($"create_time"))
    .select($"create_time", $"m", $"id", $"value")
    .writeTo("test_db.tbl")
    .append

// Pasteモードを終了し、現在解釈中です。

scala> spark.table("test_db.tbl").show
+-------------------+---+---+-----+
|        create_time|  m| id|value|
+-------------------+---+---+-----+
|2021-01-01 10:10:10|  1|  1|    1|
|2022-02-02 10:10:10|  2|  2|    2|
+-------------------+---+---+-----+

scala> spark.sql("DELETE FROM test_db.tbl WHERE id=1")
res3: org.apache.spark.sql.DataFrame = []

scala> spark.table("test_db.tbl").show
+-------------------+---+---+-----+
|        create_time|  m| id|value|
+-------------------+---+---+-----+
|2022-02-02 10:10:10|  2|  2|    2|
+-------------------+---+---+-----+
```

ClickHouseのネイティブSQLを実行します。

```
scala> val options = Map(
     |     "host" -> "clickhouse",
     |     "protocol" -> "http",
     |     "http_port" -> "8123",
     |     "user" -> "default",
     |     "password" -> ""
     | )

scala> val sql = """
     | |CREATE TABLE test_db.person (
     | |  id    Int64,
     | |  name  String,
     | |  age Nullable(Int32)
     | |)
     | |ENGINE = MergeTree()
     | |ORDER BY id
     | """.stripMargin

scala> spark.executeCommand("com.clickhouse.spark.ClickHouseCommandRunner", sql, options) 

scala> spark.sql("show tables in clickhouse_s1r1.test_db").show
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|  test_db|   person|      false|
+---------+---------+-----------+

scala> spark.table("clickhouse_s1r1.test_db.person").printSchema
root
 |-- id: long (nullable = false)
 |-- name: string (nullable = false)
 |-- age: integer (nullable = true)
```

## サポートされているデータ型

このセクションでは、SparkとClickHouse間のデータ型のマッピングについて説明します。以下の表は、ClickHouseからSparkにデータを読み取る際のデータ型の変換や、SparkからClickHouseにデータを挿入する際の変換のクイックリファレンスを提供します。

### ClickHouseからSparkへのデータ読み込み

| ClickHouse データ型                                              | Spark データ型                 | サポートされている | プリミティブか  | メモ                                       |
|-----------------------------------------------------------------|-------------------------------|-------------------|--------------|-------------------------------------------|
| `Nothing`                                                       | `NullType`                    | ✅               | Yes          |                                           |
| `Bool`                                                          | `BooleanType`                 | ✅               | Yes          |                                           |
| `UInt8`, `Int16`                                                | `ShortType`                   | ✅               | Yes          |                                           |
| `Int8`                                                          | `ByteType`                    | ✅               | Yes          |                                           |
| `UInt16`,`Int32`                                                | `IntegerType`                 | ✅               | Yes          |                                           |
| `UInt32`,`Int64`, `UInt64`                                      | `LongType`                    | ✅               | Yes          |                                           |
| `Int128`,`UInt128`, `Int256`, `UInt256`                         | `DecimalType(38, 0)`          | ✅               | Yes          |                                           |
| `Float32`                                                       | `FloatType`                   | ✅               | Yes          |                                           |
| `Float64`                                                       | `DoubleType`                  | ✅               | Yes          |                                           |
| `String`, `JSON`, `UUID`, `Enum8`, `Enum16`, `IPv4`, `IPv6`     | `StringType`                  | ✅               | Yes          |                                           |
| `FixedString`                                                   | `BinaryType`, `StringType`    | ✅               | Yes          | 設定 `READ_FIXED_STRING_AS` で制御されます |
| `Decimal`                                                       | `DecimalType`                 | ✅               | Yes          | 精度とスケールは `Decimal128` まで可能    |
| `Decimal32`                                                     | `DecimalType(9, scale)`       | ✅               | Yes          |                                           |
| `Decimal64`                                                     | `DecimalType(18, scale)`      | ✅               | Yes          |                                           |
| `Decimal128`                                                    | `DecimalType(38, scale)`      | ✅               | Yes          |                                           |
| `Date`, `Date32`                                                | `DateType`                    | ✅               | Yes          |                                           |
| `DateTime`, `DateTime32`, `DateTime64`                          | `TimestampType`               | ✅               | Yes          |                                           |
| `Array`                                                         | `ArrayType`                   | ✅               | No           | 配列要素の型も変換されます               |
| `Map`                                                           | `MapType`                     | ✅               | No           | キーは `StringType` に制限されます        |
| `IntervalYear`                                                  | `YearMonthIntervalType(Year)` | ✅               | Yes          |                                           |
| `IntervalMonth`                                                 | `YearMonthIntervalType(Month)`| ✅               | Yes          |                                           |
| `IntervalDay`, `IntervalHour`, `IntervalMinute`, `IntervalSecond`| `DayTimeIntervalType`         | ✅               | No           | 特定の間隔型が使用されます               |
| `Object`                                                        |                               | ❌               |              |                                           |
| `Nested`                                                        |                               | ❌               |              |                                           |
| `Tuple`                                                         |                               | ❌               |              |                                           |
| `Point`                                                         |                               | ❌               |              |                                           |
| `Polygon`                                                       |                               | ❌               |              |                                           |
| `MultiPolygon`                                                  |                               | ❌               |              |                                           |
| `Ring`                                                          |                               | ❌               |              |                                           |
| `IntervalQuarter`                                               |                               | ❌               |              |                                           |
| `IntervalWeek`                                                  |                               | ❌               |              |                                           |
| `Decimal256`                                                    |                               | ❌               |              |                                           |
| `AggregateFunction`                                             |                               | ❌               |              |                                           |
| `SimpleAggregateFunction`                                       |                               | ❌               |              |                                           |

### SparkからClickHouseへのデータ挿入

| Spark データ型                         | ClickHouse データ型 | サポートされている | プリミティブか  | メモ                                   |
|--------------------------------------|--------------------|-------------------|--------------|--------------------------------------|
| `BooleanType`                        | `UInt8`            | ✅               | Yes          |                                      |
| `ByteType`                           | `Int8`             | ✅               | Yes          |                                      |
| `ShortType`                          | `Int16`            | ✅               | Yes          |                                      |
| `IntegerType`                        | `Int32`            | ✅               | Yes          |                                      |
| `LongType`                           | `Int64`            | ✅               | Yes          |                                      |
| `FloatType`                          | `Float32`          | ✅               | Yes          |                                      |
| `DoubleType`                         | `Float64`          | ✅               | Yes          |                                      |
| `StringType`                         | `String`           | ✅               | Yes          |                                      |
| `VarcharType`                        | `String`           | ✅               | Yes          |                                      |
| `CharType`                           | `String`           | ✅               | Yes          |                                      |
| `DecimalType`                        | `Decimal(p, s)`    | ✅               | Yes          | 精度とスケールは `Decimal128` まで可能|
| `DateType`                           | `Date`             | ✅               | Yes          |                                      |
| `TimestampType`                      | `DateTime`         | ✅               | Yes          |                                      |
| `ArrayType` (リスト、タプル、配列)   | `Array`            | ✅               | No           | 配列要素の型も変換されます           |
| `MapType`                            | `Map`              | ✅               | No           | キーは `StringType` に制限されます    |
| `Object`                             |                    | ❌               |              |                                      |
| `Nested`                             |                    | ❌               |              |                                      |

## Spark JDBC

Sparkでサポートされているデータソースの中で最もよく使われるのがJDBCです。このセクションでは、Sparkで[ClickHouse公式JDBCコネクタ](https://github.com/ClickHouse/clickhouse-java)を使用する方法について詳しく説明します。

### データの読み込み

```java
public static void main(String[] args) {
        // Sparkセッションの初期化
        SparkSession spark = SparkSession.builder().appName("example").master("local").getOrCreate();

        // JDBC接続の詳細
        String jdbcUrl = "jdbc:ch://localhost:8123/default";
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", "default");
        jdbcProperties.put("password", "123456");

        // ClickHouseからテーブルを読み込む
        Dataset<Row> df = spark.read().jdbc(jdbcUrl, "example_table", jdbcProperties);

        // DataFrameを表示する
        df.show();

        // Sparkセッションを停止する
        spark.stop();
    }
```

### データの書き込み

:::important
現時点では、JDBCを使用して既存のテーブルにのみデータを挿入できます。
:::

```java
    public static void main(String[] args) {
        // Sparkセッションの初期化
        SparkSession spark = SparkSession.builder().appName("example").master("local").getOrCreate();

        // JDBC接続の詳細
        String jdbcUrl = "jdbc:ch://localhost:8123/default";
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", "default");
        jdbcProperties.put("password", "******");
        // サンプルDataFrameの作成
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        });
        
        List<Row> rows = new ArrayList<Row>();
        rows.add(RowFactory.create(1, "John"));
        rows.add(RowFactory.create(2, "Doe"));

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        df.write()
                .mode(SaveMode.Append)
                .jdbc(jdbcUrl, "my_table", jdbcProperties);
        // DataFrameを表示する
        df.show();

        // Sparkセッションを停止する
        spark.stop();
    }
```



:::important
Spark JDBCを使用する場合、Sparkは単一パーティションでデータを読み取ります。より高い並行性を達成するには、`partitionColumn`、`lowerBound`、`upperBound`、`numPartitions`を指定する必要があります。これらは、複数のワーカーから並行して読み取る際のテーブル分割を説明します。より詳細な情報については、Apache Sparkの公式ドキュメントで[JDBC構成](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option)をご確認ください。
:::

