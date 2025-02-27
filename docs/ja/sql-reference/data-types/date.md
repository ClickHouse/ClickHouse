---
slug: /ja/sql-reference/data-types/date
sidebar_position: 12
sidebar_label: Date
---

# Date

日付。2バイトで1970-01-01からの日数として保存されます（符号なし）。Unixエポックの開始直後から、コンパイル時に定義される上限（現時点では2149年まで、完全サポートされている最終年は2148年）までの値を保存できます。

サポートされている値の範囲は: \[1970-01-01, 2149-06-06\]です。

日付の値はタイムゾーンなしで保存されます。

**例**

`Date`型のカラムを持つテーブルを作成し、データを挿入する:

``` sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- 日付を解析
-- - 文字列から、
-- - 1970-01-01からの日数として解釈される「小さな」整数から、
-- - 1970-01-01からの秒数として解釈される「大きな」整数から。
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**関連情報**

- [日付と時間を扱う関数](../../sql-reference/functions/date-time-functions.md)
- [日付と時間を扱うオペレーター](../../sql-reference/operators/index.md#operators-datetime)
- [`DateTime` データ型](../../sql-reference/data-types/datetime.md)
