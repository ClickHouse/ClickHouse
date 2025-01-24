---
slug: /ja/operations/system-tables/kafka_consumers
---
# kafka_consumers

Kafkaコンシューマに関する情報を含んでいます。
[Kafkaテーブルエンジン](../../engines/table-engines/integrations/kafka)（ネイティブClickHouse統合）に適用されます。

カラム:

- `database` (String) - Kafkaエンジンを持つテーブルのデータベース。
- `table` (String) - Kafkaエンジンを持つテーブルの名前。
- `consumer_id` (String) - Kafkaコンシューマの識別子。テーブルには多くのコンシューマが存在する可能性があります。`kafka_num_consumers`パラメータで指定されます。
- `assignments.topic` (Array(String)) - Kafkaトピック。
- `assignments.partition_id` (Array(Int32)) - KafkaのパーティションID。なお、パーティションには1つのコンシューマしか割り当てられません。
- `assignments.current_offset` (Array(Int64)) - 現在のオフセット。
- `exceptions.time` (Array(DateTime)) - 最近10件の例外が発生したタイムスタンプ。
- `exceptions.text` (Array(String)) - 最近10件の例外のテキスト。
- `last_poll_time` (DateTime) - 最近のポーリングのタイムスタンプ。
- `num_messages_read` (UInt64) - コンシューマによって読み取られたメッセージの数。
- `last_commit_time` (DateTime) - 最近のポーリングのタイムスタンプ。
- `num_commits` (UInt64) - コンシューマのコミットの総数。
- `last_rebalance_time` (DateTime) - 最近のKafkaリバランスのタイムスタンプ。
- `num_rebalance_revocations` (UInt64) - コンシューマからパーティションが取り消された回数。
- `num_rebalance_assignments` (UInt64) - コンシューマがKafkaクラスターに割り当てられた回数。
- `is_currently_used` (UInt8) - コンシューマが使用中かどうか。
- `last_used` (UInt64) - このコンシューマが最後に使用された時間、マイクロ秒でのUnix時間。
- `rdkafka_stat` (String) - ライブラリ内部の統計情報。詳しくはhttps://github.com/ClickHouse/librdkafka/blob/master/STATISTICS.md を参照してください。`statistics_interval_ms`を0に設定すると無効になり、デフォルトは3000（3秒ごとに1回）です。

例:

``` sql
SELECT *
FROM system.kafka_consumers
FORMAT Vertical
```

``` text
行 1:
──────
database:                   test
table:                      kafka
consumer_id:                ClickHouse-instance-test-kafka-1caddc7f-f917-4bb1-ac55-e28bd103a4a0
assignments.topic:          ['system_kafka_cons']
assignments.partition_id:   [0]
assignments.current_offset: [18446744073709550615]
exceptions.time:            []
exceptions.text:            []
last_poll_time:             2006-11-09 18:47:47
num_messages_read:          4
last_commit_time:           2006-11-10 04:39:40
num_commits:                1
last_rebalance_time:        1970-01-01 00:00:00
num_rebalance_revocations:  0
num_rebalance_assignments:  1
is_currently_used:          1
rdkafka_stat:               {...}

```
