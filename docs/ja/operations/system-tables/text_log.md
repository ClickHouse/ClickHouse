---
slug: /ja/operations/system-tables/text_log
---
# text_log

ログエントリを含んでいます。このテーブルに出力されるログレベルは、`text_log.level`サーバー設定で制限することができます。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行するサーバーのホスト名。
- `event_date` (Date) — エントリの日付。
- `event_time` (DateTime) — エントリの時刻。
- `event_time_microseconds` (DateTime64) — マイクロ秒単位の精度でのエントリの時刻。
- `microseconds` (UInt32) — エントリのマイクロ秒。
- `thread_name` (String) — ロギングが行われたスレッドの名前。
- `thread_id` (UInt64) — OSのスレッドID。
- `level` (`Enum8`) — エントリのレベル。可能な値:
    - `1` または `'Fatal'`。
    - `2` または `'Critical'`。
    - `3` または `'Error'`。
    - `4` または `'Warning'`。
    - `5` または `'Notice'`。
    - `6` または `'Information'`。
    - `7` または `'Debug'`。
    - `8` または `'Trace'`。
- `query_id` (String) — クエリのID。
- `logger_name` (LowCardinality(String)) — ロガーの名前（例: `DDLWorker`）。
- `message` (String) — メッセージ自体。
- `revision` (UInt32) — ClickHouseのリビジョン。
- `source_file` (LowCardinality(String)) — ロギングが行われたソースファイル。
- `source_line` (UInt64) — ロギングが行われたソース行。
- `message_format_string` (LowCardinality(String)) — メッセージのフォーマットに使用されたフォーマット文字列。
- `value1` (String) - メッセージのフォーマットに使用された引数1。
- `value2` (String) - メッセージのフォーマットに使用された引数2。
- `value3` (String) - メッセージのフォーマットに使用された引数3。
- `value4` (String) - メッセージのフォーマットに使用された引数4。
- `value5` (String) - メッセージのフォーマットに使用された引数5。
- `value6` (String) - メッセージのフォーマットに使用された引数6。
- `value7` (String) - メッセージのフォーマットに使用された引数7。
- `value8` (String) - メッセージのフォーマットに使用された引数8。
- `value9` (String) - メッセージのフォーマットに使用された引数9。
- `value10` (String) - メッセージのフォーマットに使用された引数10。

**例**

``` sql
SELECT * FROM system.text_log LIMIT 1 \G
```

``` text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2020-09-10
event_time:              2020-09-10 11:23:07
event_time_microseconds: 2020-09-10 11:23:07.871397
microseconds:            871397
thread_name:             clickhouse-serv
thread_id:               564917
level:                   Information
query_id:
logger_name:             DNSCacheUpdater
message:                 Update period 15 seconds
revision:                54440
source_file:             /ClickHouse/src/Interpreters/DNSCacheUpdater.cpp; void DB::DNSCacheUpdater::start()
source_line:             45
message_format_string:   Update period {} seconds
value1:                  15
value2:                  
value3:                  
value4:                  
value5:                  
value6:                  
value7:                  
value8:                  
value9:                  
value10:                  
```
