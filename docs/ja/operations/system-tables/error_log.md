---
slug: /ja/operations/system-tables/error_log
---
# error_log

`system.errors`テーブルから得られるエラー値の履歴を保持しており、一定間隔でディスクに書き出されます。

カラム:
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行したサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — イベントの日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — イベントの時刻。
- `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — エラーのコード番号。
- `error` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - エラーの名称。
- `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — このエラーが発生した回数。
- `remote` ([UInt8](../../sql-reference/data-types/int-uint.md)) — リモート例外（すなわち、分散クエリの一つで受け取られたもの）。

**例**

``` sql
SELECT * FROM system.error_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2024-06-18
event_time: 2024-06-18 07:32:39
code:       999
error:      KEEPER_EXCEPTION
value:      2
remote:     0
```

**関連項目**

- [error_log 設定](../../operations/server-configuration-parameters/settings.md#error_log) — 設定の有効化および無効化について。
- [system.errors](../../operations/system-tables/errors.md) — エラーコードとそれがトリガーされた回数を含む。
- [監視](../../operations/monitoring.md) — ClickHouseの監視の基本概念。
