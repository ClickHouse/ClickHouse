---
slug: /ja/sql-reference/window-functions/lagInFrame
sidebar_label: lagInFrame
sidebar_position: 9
---

# lagInFrame

順序付けられたフレーム内で現在の行の前に指定された物理的なオフセット行にある評価された値を返します。

**構文**

```sql
lagInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS または RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

ウィンドウ関数の構文の詳細については以下を参照してください: [ウィンドウ関数 - 構文](./index.md/#syntax).

**パラメータ**
- `x` — カラム名。
- `offset` — 適用するオフセット。[(U)Int*](../data-types/int-uint.md)。 (オプション - デフォルトで `1`)。
- `default` — 計算された行がウィンドウフレームの境界を超えた場合に返される値。 (オプション - 省略時はカラムタイプのデフォルト値)。

**返される値**

- 順序付けられたフレーム内で現在の行の前に指定された物理的なオフセット行にある評価された値。

**例**

この例では特定の株式の過去データを見て、`lagInFrame` 関数を使用して株価の終値の日々の変化と変化率を計算します。

クエリ:

```sql
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- 始値
    `high`   Float32, -- 日中高値
    `low`    Float32, -- 日中安値
    `close`  Float32, -- 終値
    `volume` UInt32   -- 取引量
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql
SELECT
    date,
    close,
    lagInFrame(close, 1, close) OVER (ORDER BY date ASC) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC;
```

結果:

```response
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```
