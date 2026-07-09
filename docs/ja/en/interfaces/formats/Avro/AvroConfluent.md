---
alias: []
description: 'AvroConfluent フォーマットに関するドキュメント'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

[Apache Avro](https://avro.apache.org/) は、効率的なデータ処理のためにバイナリエンコーディングを使用する行指向のシリアライゼーション形式です。`AvroConfluent` フォーマットは、[Confluent スキーマレジストリ](https://docs.confluent.io/current/schema-registry/index.html) (または API 互換サービス) を使用して、Avro エンコードされたメッセージの読み書きをサポートします。

各メッセージは Confluent のワイヤ形式を使用します。これは、マジックバイト (`0x00`) に続いて 4 バイトのビッグエンディアンのスキーマ ID があり、その後に Avro のバイナリデータが続く形式です。読み取り時には、ClickHouse がレジストリにクエリを送信してスキーマ ID を解決します。書き込み時には、ClickHouse が出力カラムから導出したスキーマを登録し、生成された ID を各行の先頭に付加します。最適なパフォーマンスを得るため、スキーマは cache されます。

<a id="data-types-matching" />

<div id="data-type-mapping">
  ## データ型のマッピング
</div>

<DataTypesMatching />

<div id="format-settings">
  ## フォーマット設定
</div>

[//]: # "NOTE これらの設定はセッションレベルでも設定できますが、一般的ではないため、あまり目立つ形で記載するとユーザーの混乱を招く可能性があります。"

| Setting                                          | Description                                                                                 | Default |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------- | ------- |
| `input_format_avro_allow_missing_fields`         | フィールドがスキーマ内に見つからない場合に、エラーを発生させる代わりにデフォルト値を使用するかどうか。                                         | `0`     |
| `input_format_avro_null_as_default`              | `null` 値を NULL を許可しないカラムに挿入する際に、エラーを発生させる代わりにデフォルト値を使用するかどうか。                               | `0`     |
| `format_avro_schema_registry_url`                | Confluent スキーマレジストリの URL。基本認証を使用する場合は、URL エンコードした認証情報を URL パスに直接含めることができます。              |         |
| `format_avro_schema_registry_connection_timeout` | スキーマレジストリ HTTP クライアントの接続 timeout (秒) 。スキーマの取得と登録の両方で使用されます。0 より大きく、600 (10 分) 未満である必要があります。 | `1`     |
| `format_avro_schema_registry_send_timeout`       | スキーマレジストリ HTTP クライアントの送信 timeout (秒) 。0 より大きく、600 (10 分) 未満である必要があります。                      | `1`     |
| `format_avro_schema_registry_receive_timeout`    | スキーマレジストリ HTTP クライアントの受信 timeout (秒) 。0 より大きく、600 (10 分) 未満である必要があります。                      | `1`     |
| `output_format_avro_confluent_subject`           | 出力用: スキーマレジストリでスキーマを登録する subject 名。書き込み時に必須です。                                               |         |
| `output_format_avro_string_column_pattern`       | 出力用: Avro `string` としてシリアライズする String 型のカラムを指定する正規表現 (デフォルトは `bytes`) 。                     |         |

<div id="examples">
  ## 例
</div>

<div id="reading-from-kafka">
  ### Kafka からの読み取り
</div>

[Kafka テーブルエンジン](/ja/engines/table-engines/integrations/kafka.md) を使用して Avro エンコードされた Kafka トピックを読み取るには、`format_avro_schema_registry_url` 設定でスキーマレジストリの URL を指定します。

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic1_stream;
```

<div id="writing-to-kafka">
  ### Kafka への書き込み
</div>

AvroConfluent メッセージを Kafkaトピックに書き込むには、スキーマレジストリの URL と subject 名の両方を設定します。スキーマは最初の書き込み時に自動的にレジストリに自動登録されます。

```sql
CREATE TABLE topic1_sink
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url',
output_format_avro_confluent_subject = 'topic1-value';

INSERT INTO topic1_sink VALUES ('hello', 'world');
```

<div id="using-basic-authentication">
  #### 基本認証を使用する
</div>

スキーマレジストリで基本認証が必要な場合 (たとえば Confluent Cloud を使用している場合) は、`format_avro_schema_registry_url` 設定に URL エンコードした認証情報を指定できます。

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'https://<username>:<password>@schema-registry-url';
```

<div id="troubleshooting">
  ## トラブルシューティング
</div>

インジェストの進行状況を監視し、Kafka コンシューマーのエラーをデバッグするには、[`system.kafka_consumers` システムテーブル](../../../operations/system-tables/kafka_consumers.md)をクエリして確認できます。デプロイ環境に複数のレプリカがある場合 (例: ClickHouse Cloud) は、[`clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md) テーブル関数を使用する必要があります。

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

スキーマの解決に関する問題が発生した場合は、[kafkacat](https://github.com/edenhill/kafkacat) と [clickhouse-local](/ja/operations/utilities/clickhouse-local.md) を使ってトラブルシュートできます。

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```