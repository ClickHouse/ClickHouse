---
slug: /ja/operations/system-tables/processes
---
# processes

このシステムテーブルは、`SHOW PROCESSLIST` クエリを実装するために使用されます。

カラム:

- `user` (String) – クエリを実行したユーザー。分散処理の場合、クエリは `default` ユーザーでリモートサーバーに送信されます。このフィールドには特定のクエリのユーザー名が含まれ、このクエリが起動したクエリのユーザー名は含まれません。
- `address` (String) – リクエストが行われたIPアドレス。分散処理でも同様です。分散クエリが最初にどこで行われたかを追跡するには、クエリをリクエストしたサーバーの `system.processes` を確認してください。
- `elapsed` (Float64) – リクエスト実行が開始されてからの経過時間（秒）。
- `read_rows` (UInt64) – テーブルから読み取った行の数。分散処理の場合、リクエスタサーバーでは、すべてのリモートサーバーの合計となります。
- `read_bytes` (UInt64) – テーブルから読み取った非圧縮バイト数。分散処理の場合、リクエスタサーバーでは、すべてのリモートサーバーの合計となります。
- `total_rows_approx` (UInt64) – 読み取るべき総行数の概算。分散処理の場合、リクエスタサーバーでは、すべてのリモートサーバーの合計となります。新しい処理対象が判明したときに、リクエスト処理中に更新されることがあります。
- `memory_usage` (Int64) – リクエストが使用するRAMの量。専用メモリの一部のタイプは含まれない場合があります。[max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage) 設定を参照してください。
- `query` (String) – クエリテキスト。`INSERT`の場合、挿入するデータは含まれません。
- `query_id` (String) – クエリIDが定義されている場合。
- `is_cancelled` (UInt8) – クエリがキャンセルされたかどうか。
- `is_all_data_sent` (UInt8) – クライアントにすべてのデータが送信されたかどうか（つまり、クエリがサーバー上で完了したかどうか）。

```sql
SELECT * FROM system.processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
is_initial_query:     1
user:                 default
query_id:             35a360fa-3743-441d-8e1f-228c938268da
address:              ::ffff:172.23.0.1
port:                 47588
initial_user:         default
initial_query_id:     35a360fa-3743-441d-8e1f-228c938268da
initial_address:      ::ffff:172.23.0.1
initial_port:         47588
interface:            1
os_user:              bharatnc
client_hostname:      tower
client_name:          ClickHouse
client_revision:      54437
client_version_major: 20
client_version_minor: 7
client_version_patch: 2
http_method:          0
http_user_agent:
quota_key:
elapsed:              0.000582537
is_cancelled:         0
is_all_data_sent:     0
read_rows:            0
read_bytes:           0
total_rows_approx:    0
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
query:                SELECT * from system.processes LIMIT 10 FORMAT Vertical;
thread_ids:           [67]
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
Settings:             {'background_pool_size':'32','load_balancing':'random','allow_suspicious_low_cardinality_types':'1','distributed_aggregation_memory_efficient':'1','skip_unavailable_shards':'1','log_queries':'1','max_bytes_before_external_group_by':'20000000000','max_bytes_before_external_sort':'20000000000','allow_introspection_functions':'1'}

1 rows in set. Elapsed: 0.002 sec.
```
