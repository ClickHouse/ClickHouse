---
slug: /ja/operations/system-tables/query_thread_log
---
# query_thread_log

クエリを実行するスレッドに関する情報を含んでいます。例えば、スレッド名、スレッドの開始時間、クエリ処理の所要時間などです。

ログ記録を開始するには：

1. [query_thread_log](../../operations/server-configuration-parameters/settings.md#query_thread_log) セクションでパラメーターを設定します。
2. [log_query_threads](../../operations/settings/settings.md#log-query-threads) を1に設定します。

データのフラッシュ期間は、[query_thread_log](../../operations/server-configuration-parameters/settings.md#query_thread_log) サーバー設定セクションの `flush_interval_milliseconds` パラメーターで設定します。フラッシュを強制するには、[SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) クエリを使用します。

ClickHouse はテーブルからデータを自動的に削除しません。詳細は[紹介](../../operations/system-tables/index.md#system-tables-introduction)を参照してください。

`query_thread_log` テーブルに登録されるクエリの数を減らすには、[log_queries_probability](../../operations/settings/settings.md#log-queries-probability) 設定を使用できます。

カラム：

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行するサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — スレッドがクエリの実行を終了した日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — スレッドがクエリの実行を終了した日時。
- `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — マイクロ秒精度でスレッドがクエリの実行を終了した日時。
- `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — クエリ実行の開始時間。
- `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — マイクロ秒精度でのクエリ実行の開始時間。
- `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリ実行の所要時間。
- `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 読み取られた行数。
- `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 読み取られたバイト数。
- `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `INSERT` クエリの場合、書き込まれた行数。他のクエリの場合は、このカラムの値は0です。
- `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `INSERT` クエリの場合、書き込まれたバイト数。他のクエリの場合は、このカラムの値は0です。
- `memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — このスレッドのコンテキストで割り当てられたメモリと解放されたメモリの差。
- `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — このスレッドのコンテキストで割り当てられたメモリと解放されたメモリの最大差。
- `thread_name` ([String](../../sql-reference/data-types/string.md)) — スレッド名。
- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — OSスレッドID。
- `master_thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 初期スレッドのOS初期ID。
- `query` ([String](../../sql-reference/data-types/string.md)) — クエリ文字列。
- `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリタイプ。可能な値：
    - 1 — クライアントによって開始されたクエリ。
    - 0 — 分散クエリ実行のために別のクエリによって開始されたクエリ。
- `user` ([String](../../sql-reference/data-types/string.md)) — 現在のクエリを開始したユーザー名。
- `query_id` ([String](../../sql-reference/data-types/string.md)) — クエリのID。
- `address` ([IPv6](../../sql-reference/data-types/ipv6.md)) — クエリの実行に使用されたIPアドレス。
- `port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリの実行に使用されたクライアントポート。
- `initial_user` ([String](../../sql-reference/data-types/string.md)) — 初期クエリを実行したユーザー名（分散クエリ実行の場合）。
- `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — 初期クエリのID（分散クエリ実行の場合）。
- `initial_address` ([IPv6](../../sql-reference/data-types/ipv6.md)) — 親クエリが起動されたIPアドレス。
- `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 親クエリの実行に使用されたクライアントポート。
- `interface` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリが開始されたインターフェース。可能な値：
    - 1 — TCP。
    - 2 — HTTP。
- `os_user` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) を実行しているOSのユーザー名。
- `client_hostname` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) または別のTCPクライアントが実行されているクライアントマシンのホスト名。
- `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) または別のTCPクライアント名。
- `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) または別のTCPクライアントのリビジョン。
- `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) または別のTCPクライアントのメジャーバージョン。
- `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) または別のTCPクライアントのマイナーバージョン。
- `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) または別のTCPクライアントのパッチバージョン。
- `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリを開始したHTTPメソッド。可能な値：
    - 0 — クエリがTCPインターフェースから開始されました。
    - 1 — `GET` メソッドが使用されました。
    - 2 — `POST` メソッドが使用されました。
- `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — HTTPリクエストで渡された `UserAgent` ヘッダー。
- `quota_key` ([String](../../sql-reference/data-types/string.md)) — [quota](../../operations/quotas.md) 設定で指定された「クオータキー」（`keyed` を参照）。
- `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouseのリビジョン。
- `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — このスレッドの異なるメトリクスを測定するProfileEvents。これらの説明は、[system.events](#system_tables-events) テーブルで見つけることができます。

**例**

``` sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

``` text
行 1:
──────
hostname:                      clickhouse.eu-central1.internal
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.134042
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063150
query_duration_ms:             70
read_rows:                     0
read_bytes:                    0
written_rows:                  1
written_bytes:                 12
memory_usage:                  4300844
peak_memory_usage:             4300844
thread_name:                   TCPHandler
thread_id:                     638133
master_thread_id:              638133
query:                         INSERT INTO test1 VALUES
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:
quota_key:
revision:                      54440
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
```

**関連項目**

- [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — クエリ実行に関する一般情報を含む `query_log` システムテーブルの説明。
- [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) — クエリ中に実行された各ビューに関する情報を含むテーブル。
