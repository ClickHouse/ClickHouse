---
description: 'nonNegativeDerivative ウィンドウ関数のドキュメント'
sidebar_label: 'nonNegativeDerivative'
sidebar_position: 12
slug: /sql-reference/window-functions/nonNegativeDerivative
title: 'nonNegativeDerivative'
doc_type: 'reference'
---

`timestamp_column` に対する `metric_column` の非負の導関数を計算します。
これは ClickHouse 固有のウィンドウ関数であり、標準 SQL の一部ではありません。

各行の導関数は、*ウィンドウの評価順序における直前の行*を基準に計算されます。この評価順序はウィンドウの `ORDER BY` 句によって決まるものであり、`timestamp_column` によるものではありません。
`timestamp_column` 引数は、現在の行と直前の行の間の経過時間を測定するためにのみ参照され、行の順序付けには使用されません。

:::warning
`nonNegativeDerivative` は `timestamp_column` で行を並べ替えません。並べ替えはウィンドウの `ORDER BY` が担います。
以下の計算式を適用するには、ウィンドウの評価順序において `timestamp_column` が厳密に単調増加している必要があります。そのため、通常はウィンドウを `timestamp_column` の昇順で並べ替えてください（例：`nonNegativeDerivative(metric, ts)` と組み合わせて `... OVER (ORDER BY ts ASC)` を使用）。
現在の行と直前の行の間の経過時間が非正の場合（`ORDER BY timestamp_column DESC` を指定した場合や、タイムスタンプが重複している場合に発生）、関数は計算式を適用する代わりにその行に対して `0` を返します。
:::

結果は `INTERVAL` あたりのメトリクスの変化率であり、負の値はすべて `0` にクランプされます。
これは、カウンターのような単調増加するメトリクスに特に有用です。値の減少は通常、実際の負の変化率ではなくリセットを意味するためです。

**Syntax**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name AS ([PARTITION BY grouping_column] [ORDER BY sorting_column] [ROWS or RANGE expression_to_bound_rows_within_the_group])
```

ウィンドウ関数の構文の詳細については、[ウィンドウ関数 - 構文](./index.md/#syntax)を参照してください。

**Arguments**

- `metric_column` — 導関数を計算する対象のカラム。[(U)Int*](../data-types/int-uint.md) または [Float*](../data-types/float.md)。
- `timestamp_column` — ウィンドウ順序における現在の行と直前の行の間の経過時間を測定するために使用するカラム。行の順序付けには使用されず、順序付けはウィンドウの `ORDER BY` が担います。通常は同じカラムを指定してください。[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。
- `INTERVAL X UNITS` — 省略可能。結果をスケーリングする時間単位。デフォルトは `INTERVAL 1 SECOND`。固定長の単位のみサポートされます（`NANOSECOND`、`MICROSECOND`、`MILLISECOND`、`SECOND`、`MINUTE`、`HOUR`、`DAY`、`WEEK`）。可変長の単位（`MONTH`、`QUARTER`、`YEAR`）を指定すると例外が発生します。

**Returned value**

各行の値は次のように計算されます：

- 最初の行は `0`；
- 直前の行からの経過時間が非正の行（すなわち $\text{timestamp}_i - \text{timestamp}_{i-1} \le 0$、降順または重複タイムスタンプの場合に発生）は `0`；
- それ以外の場合は ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$。

計算結果が負になる場合は `0` にクランプされます。戻り値の型は [Float64](../data-types/float.md) です。

**Example**

次の例では、センサーの読み取り値の1秒あたりの変化率を計算します。
3行目の値は `110` から `105` に減少しているため、その導関数は `0` にクランプされます。

```sql title="Query"
CREATE TABLE sensor_readings
(
    `sensor_id` UInt32,
    `ts`        DateTime,
    `reading`   Float64
)
ENGINE = Memory;

INSERT INTO sensor_readings VALUES
    (1, '2024-01-01 00:00:00', 100),
    (1, '2024-01-01 00:00:10', 110),
    (1, '2024-01-01 00:00:20', 105),
    (1, '2024-01-01 00:00:30', 130);
```

```sql title="Query"
SELECT
    ts,
    reading,
    nonNegativeDerivative(reading, ts) OVER (ORDER BY ts ASC) AS deriv_per_second
FROM sensor_readings
ORDER BY ts ASC;
```

```response title="Response"
   ┌──────────────────ts─┬─reading─┬─deriv_per_second─┐
1. │ 2024-01-01 00:00:00 │     100 │                0 │
2. │ 2024-01-01 00:00:10 │     110 │                1 │
3. │ 2024-01-01 00:00:20 │     105 │                0 │
4. │ 2024-01-01 00:00:30 │     130 │              2.5 │
   └─────────────────────┴─────────┴──────────────────┘
```