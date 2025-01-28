---
sidebar_label: Apache Beam
slug: /ja/integrations/apache-beam
description: Apache Beamを使用したClickHouseへのデータ取り込み
---

# Apache BeamとClickHouseの統合

**Apache Beam**は、オープンソースの統合プログラミングモデルで、開発者がバッチおよびストリーム（継続的）データ処理パイプラインの定義と実行を可能にします。Apache Beamの柔軟性は、ETL（抽出、変換、ロード）操作から複雑なイベント処理およびリアルタイム解析まで、幅広いデータ処理シナリオをサポートする能力にあります。この統合は、基盤の挿入レイヤーとしてClickHouseの公式[JDBCコネクタ](https://github.com/ClickHouse/clickhouse-java)を活用します。

## 統合パッケージ

Apache BeamとClickHouseを統合するために必要な統合パッケージは、多くの一般的なデータストレージシステムおよびデータベースの統合バンドル[Apache Beam I/O Connectors](https://beam.apache.org/documentation/io/connectors/)として管理および開発されています。`org.apache.beam.sdk.io.clickhouse.ClickHouseIO`の実装は、[Apache Beamリポジトリ](https://github.com/apache/beam/tree/0bf43078130d7a258a0f1638a921d6d5287ca01e/sdks/java/io/clickhouse/src/main/java/org/apache/beam/sdk/io/clickhouse)にあります。

## Apache Beam ClickHouseパッケージのセットアップ

### パッケージインストール

以下の依存関係をパッケージ管理フレームワークに追加します:
```xml
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-clickhouse</artifactId>
    <version>${beam.version}</version>
</dependency>
```

アーティファクトは[公式マヴンリポジトリ](https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-io-clickhouse)で見つけることができます。

### コード例

以下の例では、`input.csv`という名前のCSVファイルを`PCollection`として読み込み、定義されたスキーマを使用して行オブジェクトに変換し、`ClickHouseIO`を使用してローカルのClickHouseインスタンスに挿入します：

```java
package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

public class Main {

    public static void main(String[] args) {
        // パイプラインオブジェクトの作成
        Pipeline p = Pipeline.create();

        Schema SCHEMA =
                Schema.builder()
                        .addField(Schema.Field.of("name", Schema.FieldType.STRING).withNullable(true))
                        .addField(Schema.Field.of("age", Schema.FieldType.INT16).withNullable(true))
                        .addField(Schema.Field.of("insertion_time", Schema.FieldType.DATETIME).withNullable(false))
                        .build();

        // パイプラインへの変換の適用
        PCollection<String> lines = p.apply("ReadLines", TextIO.read().from("src/main/resources/input.csv"));

        PCollection<Row> rows = lines.apply("ConvertToRow", ParDo.of(new DoFn<String, Row>() {
            @ProcessElement
            public void processElement(@Element String line, OutputReceiver<Row> out) {
                String[] values = line.split(",");
                Row row = Row.withSchema(SCHEMA)
                        .addValues(values[0], Short.parseShort(values[1]), DateTime.now())
                        .build();
                out.output(row);
            }
        })).setRowSchema(SCHEMA);

        rows.apply("Write to ClickHouse",
                        ClickHouseIO.write("jdbc:clickhouse://localhost:8123/default?user=default&password=******", "test_table"));

        // パイプラインを実行
        p.run().waitUntilFinish();
    }
}

```

## サポートされているデータタイプ

| ClickHouse                           | Apache Beam                  | 対応         | 備考                                                                                                                                   |
|--------------------------------------|------------------------------|--------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `TableSchema.TypeName.FLOAT32`       | `Schema.TypeName#FLOAT`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.FLOAT64`       | `Schema.TypeName#DOUBLE`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.INT8`       | `Schema.TypeName#BYTE`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.INT16`       | `Schema.TypeName#INT16`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.INT32`       | `Schema.TypeName#INT32`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.INT64`       | `Schema.TypeName#INT64`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.STRING`       | `Schema.TypeName#STRING`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.UINT8`       | `Schema.TypeName#INT16`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.UINT16`       | `Schema.TypeName#INT32`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.UINT32`       | `Schema.TypeName#INT64`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.UINT64`       | `Schema.TypeName#INT64`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.DATE`       | `Schema.TypeName#DATETIME`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.DATETIME`      | `Schema.TypeName#DATETIME`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.ARRAY`       | `Schema.TypeName#ARRAY`   | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.ENUM8`       | `Schema.TypeName#STRING`  | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.ENUM16`      | `Schema.TypeName#STRING`  | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.BOOL`        | `Schema.TypeName#BOOLEAN` | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.TUPLE`       | `Schema.TypeName#ROW`        | ✅            |                                                                                                                                        |
| `TableSchema.TypeName.FIXEDSTRING` | `FixedBytes`                 | ✅            | `FixedBytes`は、固定長のバイト配列を表すLogicalTypeであり、`org.apache.beam.sdk.schemas.logicaltypes`に位置しています |
|                                    | `Schema.TypeName#DECIMAL`    | ❌            |                                                                                                                                        |
|                                    | `Schema.TypeName#MAP`        | ❌            |                                                                                                                                        |


## 制限

コネクタを使用する際には、以下の制限事項を考慮してください：
* 現時点では、Sink操作のみがサポートされています。コネクタはSource操作をサポートしていません。
* ClickHouseは、`ReplicatedMergeTree`または`ReplicatedMergeTree`を基に構築された`分散テーブル`に挿入する際にデデュプリケーションを行います。レプリケーションがない場合、通常のMergeTreeに挿入すると、挿入が失敗し、その後再試行に成功すると重複が発生する可能性があります。ただし、各ブロックは原子的に挿入され、ブロックサイズは`ClickHouseIO.Write.withMaxInsertBlockSize(long)`を使用して設定できます。デデュプリケーションは、挿入されたブロックのチェックサムを使用して実現されます。デデュプリケーションに関する詳細は、[Deduplication](https://clickhouse.com/docs/ja/guides/developer/deduplication)および[Deduplicate insertion config](https://clickhouse.com/docs/ja/operations/settings/settings#insert-deduplicate)を参照してください。
* コネクタはDDL文を実行しないため、挿入先のテーブルはあらかじめ存在している必要があります。

## 関連コンテンツ
* `ClickHouseIO`クラスの[ドキュメント](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/clickhouse/ClickHouseIO.html)。
* `Github`リポジトリの例[clickhouse-beam-connector](https://github.com/ClickHouse/clickhouse-beam-connector)。
