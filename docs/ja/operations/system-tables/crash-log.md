---
slug: /ja/operations/system-tables/crash-log
---
# crash_log

致命的なエラーに関するスタックトレースの情報が含まれています。このテーブルはデフォルトではデータベースに存在しておらず、致命的なエラーが発生したときにのみ作成されます。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `event_date` ([DateTime](../../sql-reference/data-types/datetime.md)) — イベントの日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — イベントの時刻。
- `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ナノ秒単位のイベントのタイムスタンプ。
- `signal` ([Int32](../../sql-reference/data-types/int-uint.md)) — シグナル番号。
- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — スレッドID。
- `query_id` ([String](../../sql-reference/data-types/string.md)) — クエリID。
- `trace` ([Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クラッシュ時のスタックトレース。各要素はClickHouseサーバープロセス内の仮想メモリアドレスです。
- `trace_full` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — クラッシュ時のスタックトレース。各要素はClickHouseサーバープロセス内で呼び出されたメソッドを含みます。
- `version` ([String](../../sql-reference/data-types/string.md)) — ClickHouseサーバーのバージョン。
- `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouseサーバーのリビジョン。
- `build_id` ([String](../../sql-reference/data-types/string.md)) — コンパイラによって生成されるBuildID。

**例**

クエリ:

``` sql
SELECT * FROM system.crash_log ORDER BY event_time DESC LIMIT 1;
```

結果（完全ではありません）:

``` text
行 1:
──────
hostname:     clickhouse.eu-central1.internal
event_date:   2020-10-14
event_time:   2020-10-14 15:47:40
timestamp_ns: 1602679660271312710
signal:       11
thread_id:    23624
query_id:     428aab7c-8f5c-44e9-9607-d16b44467e69
trace:        [188531193,...]
trace_full:   ['3. DB::(anonymous namespace)::FunctionFormatReadableTimeDelta::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> >&, std::__1::vector<unsigned long, std::__1::allocator<unsigned long> > const&, unsigned long, unsigned long) const @ 0xb3cc1f9 in /home/username/work/ClickHouse/build/programs/clickhouse',...]
version:      ClickHouse 20.11.1.1
revision:     54442
build_id:
```

**関連項目**
- [trace_log](../../operations/system-tables/trace_log.md) システムテーブル

