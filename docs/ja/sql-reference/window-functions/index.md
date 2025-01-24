---
slug: /ja/sql-reference/window-functions/
sidebar_label: ウィンドウ関数
sidebar_position: 1
---

# ウィンドウ関数

ウィンドウ関数を使用すると、現在の行に関連する一連の行に対して計算を実行できます。これらの計算の一部は集約関数で実行できるものと似ていますが、ウィンドウ関数では行が単一の出力にグループ化されることはなく、個々の行が返され続けます。

## 標準ウィンドウ関数

ClickHouse はウィンドウとウィンドウ関数を定義するための標準的な文法をサポートしています。以下の表は、機能が現在サポートされているかどうかを示しています。

| 機能                                                                  | サポート状況                                                                                                                                                                       |
|--------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| アドホックウィンドウ仕様 (`count(*) over (partition by id order by time desc)`) | ✅                                                                                                                                                                                  |
| ウィンドウ関数を含む式例 (`(count(*) over ()) / 2)`)             | ✅                                                                                                                                                                                   |
| `WINDOW`句 (`select ... from table window w as (partition by id)`)            | ✅                                                                                                                                                                                   |
| `ROWS`フレーム                                                                       | ✅                                                                                                                                                                                   |
| `RANGE`フレーム                                                                      | ✅ (デフォルト)                                                                                                                                                                      |
| `DateTime` `RANGE OFFSET`フレームの`INTERVAL`構文                              | ❌ (代わりに秒数を指定してください（`RANGE`は任意の数値型で動作します）。)                                                                                                                                 |
| `GROUPS`フレーム                                                                     | ❌                                                                                                                                                                               |
| フレームを超える集約関数の計算 (`sum(value) over (order by time)`)   | ✅ (すべての集約関数がサポートされます)                                                                                                                                                       |
| `rank()`, `dense_rank()`, `row_number()`                                           | ✅ <br/>エイリアス: `denseRank()`                                                                                                                                                                                   |
| `percent_rank()` | ✅ パーティション内での値の相対的な位置を効率的に計算します。この関数は、より冗長で計算集約的な手動SQL計算である`ifNull((rank() OVER(PARTITION BY x ORDER BY y) - 1) / nullif(count(1) OVER(PARTITION BY x) - 1, 0), 0)`を効果的に置き換えます <br/>エイリアス: `percentRank()`| 
| `lag/lead(value, offset)`                                                          | ❌ <br/> 次の回避策のいずれかを使用できます:<br/> 1) `any(value) over (.... rows between <offset> preceding and <offset> preceding)`, または `lead`の場合は`following` <br/> 2) `lagInFrame/leadInFrame`, これらは類似していますが、ウィンドウフレームに従います。`lag/lead`と同じ動作を得るには、`rows between unbounded preceding and unbounded following`を使用してください。                                                                 |
| ntile(buckets) | ✅ <br/> ウィンドウを指定します (partition by x order by y rows between unbounded preceding and unbounded following)。 |

## ClickHouse固有のウィンドウ関数

以下のClickHouse固有のウィンドウ関数もあります:

### nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])

指定された`metric_column`を`timestamp_column`で非負の導関数を見つけます。
`INTERVAL`は省略可能で、デフォルトは`INTERVAL 1 SECOND`です。
各行に対して計算される値は以下の通りです:
- `0`は最初の行、
- ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}}  * \text{interval}$ は $i_{th}$ 行。

## 文法

```text
aggregate_function (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

- `PARTITION BY` - 結果セットをグループに分ける方法を定義します。
- `ORDER BY` - 集約関数の計算中にグループ内の行を並び替える方法を定義します。
- `ROWS or RANGE` - フレームの境界を定義し、集約関数はフレーム内で計算されます。
- `WINDOW` - 複数の式が同じウィンドウ定義を使用できるようにします。

```text
      PARTITION
┌─────────────────┐  <-- UNBOUNDED PRECEDING (BEGINNING of the PARTITION)
│                 │
│                 │
│=================│  <-- N PRECEDING  <─┐
│      N ROWS     │                     │  F
│  Before CURRENT │                     │  R
│~~~~~~~~~~~~~~~~~│  <-- CURRENT ROW    │  A
│     M ROWS      │                     │  M
│   After CURRENT │                     │  E
│=================│  <-- M FOLLOWING  <─┘
│                 │
│                 │
└─────────────────┘  <--- UNBOUNDED FOLLOWING (END of the PARTITION)
```

### 関数

これらの関数はウィンドウ関数としてのみ使用できます。

- [`row_number()`](./row_number.md) - 自身のパーティション内で現在の行を1から番号付けします。
- [`first_value(x)`](./first_value.md) - 順序付けられたフレーム内で最初に評価された値を返します。
- [`last_value(x)`](./last_value.md) - 順序付けられたフレーム内で最後に評価された値を返します。
- [`nth_value(x, offset)`](./nth_value.md) - 順序付けられたフレーム内のnth行（オフセット）で評価された最初の非NULL値を返します。
- [`rank()`](./rank.md) - ギャップを持つパーティション内で現在の行をランク付けします。
- [`dense_rank()`](./dense_rank.md) - ギャップなしでパーティション内で現在の行をランク付けします。
- [`lagInFrame(x)`](./lagInFrame.md) - 順序付けられたフレーム内で現在の行の前の指定された物理オフセット行で評価された値を返します。
- [`leadInFrame(x)`](./leadInFrame.md) - 順序付けられたフレーム内で現在の行の後のオフセット行で評価された値を返します。

## 例

ウィンドウ関数がどのように使用されるかをいくつかの例で見てみましょう。

### 行の番号付け

```sql
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M');
```

```sql
SELECT player, salary, 
       row_number() OVER (ORDER BY salary) AS row
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─row─┐
│ Michael Stanley │ 150000 │   1 │
│ Scott Harrison  │ 150000 │   2 │
│ Charles Juarez  │ 190000 │   3 │
│ Gary Chen       │ 195000 │   4 │
│ Robert George   │ 195000 │   5 │
└─────────────────┴────────┴─────┘
```

```sql
SELECT player, salary, 
       row_number() OVER (ORDER BY salary) AS row,
       rank() OVER (ORDER BY salary) AS rank,
       dense_rank() OVER (ORDER BY salary) AS denseRank
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─row─┬─rank─┬─denseRank─┐
│ Michael Stanley │ 150000 │   1 │    1 │         1 │
│ Scott Harrison  │ 150000 │   2 │    1 │         1 │
│ Charles Juarez  │ 190000 │   3 │    3 │         2 │
│ Gary Chen       │ 195000 │   4 │    4 │         3 │
│ Robert George   │ 195000 │   5 │    4 │         3 │
└─────────────────┴────────┴──────┴──────┴───────────┘
```

### 集計関数

各プレイヤーの給与をチームの平均と比較します。

```sql
SELECT player, salary, team,
       avg(salary) OVER (PARTITION BY team) AS teamAvg,
       salary - teamAvg AS diff
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─team──────────────────────┬─teamAvg─┬───diff─┐
│ Charles Juarez  │ 190000 │ New Coreystad Archdukes   │  170000 │  20000 │
│ Scott Harrison  │ 150000 │ New Coreystad Archdukes   │  170000 │ -20000 │
│ Gary Chen       │ 195000 │ Port Elizabeth Barbarians │  180000 │  15000 │
│ Michael Stanley │ 150000 │ Port Elizabeth Barbarians │  180000 │ -30000 │
│ Robert George   │ 195000 │ Port Elizabeth Barbarians │  180000 │  15000 │
└─────────────────┴────────┴───────────────────────────┴─────────┴────────┘
```

各プレイヤーの給与をチームの最大値と比較します。

```sql
SELECT player, salary, team,
       max(salary) OVER (PARTITION BY team) AS teamAvg,
       salary - teamAvg AS diff
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─team──────────────────────┬─teamAvg─┬───diff─┐
│ Charles Juarez  │ 190000 │ New Coreystad Archdukes   │  190000 │      0 │
│ Scott Harrison  │ 150000 │ New Coreystad Archdukes   │  190000 │ -40000 │
│ Gary Chen       │ 195000 │ Port Elizabeth Barbarians │  195000 │      0 │
│ Michael Stanley │ 150000 │ Port Elizabeth Barbarians │  195000 │ -45000 │
│ Robert George   │ 195000 │ Port Elizabeth Barbarians │  195000 │      0 │
└─────────────────┴────────┴───────────────────────────┴─────────┴────────┘
```

### カラムによるパーティション分割

```sql
CREATE TABLE wf_partition
(
    `part_key` UInt64,
    `value` UInt64,
    `order` UInt64    
)
ENGINE = Memory;

INSERT INTO wf_partition FORMAT Values
   (1,1,1), (1,2,2), (1,3,3), (2,0,0), (3,0,0);

SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key) AS frame_values
FROM wf_partition
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3]      │   <┐   
│        1 │     2 │     2 │ [1,2,3]      │    │  1つ目のグループ
│        1 │     3 │     3 │ [1,2,3]      │   <┘ 
│        2 │     0 │     0 │ [0]          │   <- 2つ目のグループ
│        3 │     0 │     0 │ [0]          │   <- 3つ目のグループ
└──────────┴───────┴───────┴──────────────┘
```

### フレーム境界設定

```sql
CREATE TABLE wf_frame
(
    `part_key` UInt64,
    `value` UInt64,
    `order` UInt64
)
ENGINE = Memory;

INSERT INTO wf_frame FORMAT Values
   (1,1,1), (1,2,2), (1,3,3), (1,4,4), (1,5,5);
```

```sql
-- フレームはパーティションの境界に囲まれています（BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING）
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
    
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [1,2,3,4,5]  │
│        1 │     4 │     4 │ [1,2,3,4,5]  │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- 簡略形式 - 境界式なし、ORDER BYなし
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [1,2,3,4,5]  │
│        1 │     4 │     4 │ [1,2,3,4,5]  │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- フレームはパーティションの始まりと現在の行で囲まれています
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        Rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [1,2,3]      │
│        1 │     4 │     4 │ [1,2,3,4]    │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- 簡略形式（フレームはパーティションの始まりと現在の行で囲まれています）
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order ASC) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [1,2,3]      │
│        1 │     4 │     4 │ [1,2,3,4]    │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- フレームはパーティションの始まりと現在の行で囲まれていますが、順序が逆です
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order DESC) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [5,4,3,2,1]  │
│        1 │     2 │     2 │ [5,4,3,2]    │
│        1 │     3 │     3 │ [5,4,3]      │
│        1 │     4 │     4 │ [5,4]        │
│        1 │     5 │     5 │ [5]          │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- スライディングフレーム - 1つ前の行と現在の行
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        Rows BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [2,3]        │
│        1 │     4 │     4 │ [3,4]        │
│        1 │     5 │     5 │ [4,5]        │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- スライディングフレーム - `Rows BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING` 
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        Rows BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [2,3,4,5]    │
│        1 │     4 │     4 │ [3,4,5]      │
│        1 │     5 │     5 │ [4,5]        │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- `row_number`はフレームを尊重しないため、`rn_1 = rn_2 = rn_3 != rn_4`
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER w1 AS frame_values,
    row_number() OVER w1 AS rn_1,
    sum(1) OVER w1 AS rn_2,
    row_number() OVER w2 AS rn_3,
    sum(1) OVER w2 AS rn_4
FROM wf_frame
WINDOW
    w1 AS (PARTITION BY part_key ORDER BY order DESC),
    w2 AS (
        PARTITION BY part_key 
        ORDER BY order DESC 
        Rows BETWEEN 1 PRECEDING AND CURRENT ROW
    )
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values─┬─rn_1─┬─rn_2─┬─rn_3─┬─rn_4─┐
│        1 │     1 │     1 │ [5,4,3,2,1]  │    5 │    5 │    5 │    2 │
│        1 │     2 │     2 │ [5,4,3,2]    │    4 │    4 │    4 │    2 │
│        1 │     3 │     3 │ [5,4,3]      │    3 │    3 │    3 │    2 │
│        1 │     4 │     4 │ [5,4]        │    2 │    2 │    2 │    2 │
│        1 │     5 │     5 │ [5]          │    1 │    1 │    1 │    1 │
└──────────┴───────┴───────┴──────────────┴──────┴──────┴──────┴──────┘
```

```sql
-- `first_value`と`last_value`はフレームを尊重します
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    first_value(value) OVER w1 AS first_value_1,
    last_value(value) OVER w1 AS last_value_1,
    groupArray(value) OVER w2 AS frame_values_2,
    first_value(value) OVER w2 AS first_value_2,
    last_value(value) OVER w2 AS last_value_2
FROM wf_frame
WINDOW
    w1 AS (PARTITION BY part_key ORDER BY order ASC),
    w2 AS (PARTITION BY part_key ORDER BY order ASC Rows BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;
┌─frame_values_1─┬─first_value_1─┬─last_value_1─┬─frame_values_2─┬─first_value_2─┬─last_value_2─┐
│ [1]            │             1 │            1 │ [1]            │             1 │            1 │
│ [1,2]          │             1 │            2 │ [1,2]          │             1 │            2 │
│ [1,2,3]        │             1 │            3 │ [2,3]          │             2 │            3 │
│ [1,2,3,4]      │             1 │            4 │ [3,4]          │             3 │            4 │
│ [1,2,3,4,5]    │             1 │            5 │ [4,5]          │             4 │            5 │
└────────────────┴───────────────┴──────────────┴────────────────┴───────────────┴──────────────┘
```

```sql
-- フレーム内の2番目の値
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    nth_value(value, 2) OVER w1 AS second_value
FROM wf_frame
WINDOW w1 AS (PARTITION BY part_key ORDER BY order ASC Rows BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC
┌─frame_values_1─┬─second_value─┐
│ [1]            │            0 │
│ [1,2]          │            2 │
│ [1,2,3]        │            2 │
│ [1,2,3,4]      │            2 │
│ [2,3,4,5]      │            3 │
└────────────────┴──────────────┘
```

```sql
-- フレーム内の2番目の値 + 欠落値へのNull
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    nth_value(toNullable(value), 2) OVER w1 AS second_value
FROM wf_frame
WINDOW w1 AS (PARTITION BY part_key ORDER BY order ASC Rows BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC
┌─frame_values_1─┬─second_value─┐
│ [1]            │         ᴺᵁᴸᴸ │
│ [1,2]          │            2 │
│ [1,2,3]        │            2 │
│ [1,2,3,4]      │            2 │
│ [2,3,4,5]      │            3 │
└────────────────┴──────────────┘
```

## 現実の例

以下は一般的な現実の問題を解決する例です。

### 部署別の最大給与/合計給与

```sql
CREATE TABLE employees
(
    `department` String,
    `employee_name` String,
    `salary` Float
)
ENGINE = Memory;

INSERT INTO employees FORMAT Values
   ('Finance', 'Jonh', 200),
   ('Finance', 'Joan', 210),
   ('Finance', 'Jean', 505),
   ('IT', 'Tim', 200),
   ('IT', 'Anna', 300),
   ('IT', 'Elen', 500);
```

```sql
SELECT
    department,
    employee_name AS emp,
    salary,
    max_salary_per_dep,
    total_salary_per_dep,
    round((salary / total_salary_per_dep) * 100, 2) AS `share_per_dep(%)`
FROM
(
    SELECT
        department,
        employee_name,
        salary,
        max(salary) OVER wndw AS max_salary_per_dep,
        sum(salary) OVER wndw AS total_salary_per_dep
    FROM employees
    WINDOW wndw AS (
        PARTITION BY department
        rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
    ORDER BY
        department ASC,
        employee_name ASC
);

┌─department─┬─emp──┬─salary─┬─max_salary_per_dep─┬─total_salary_per_dep─┬─share_per_dep(%)─┐
│ Finance    │ Jean │    505 │                505 │                  915 │            55.19 │
│ Finance    │ Joan │    210 │                505 │                  915 │            22.95 │
│ Finance    │ Jonh │    200 │                505 │                  915 │            21.86 │
│ IT         │ Anna │    300 │                500 │                 1000 │               30 │
│ IT         │ Elen │    500 │                500 │                 1000 │               50 │
│ IT         │ Tim  │    200 │                500 │                 1000 │               20 │
└────────────┴──────┴────────┴────────────────────┴──────────────────────┴──────────────────┘
```

### 累積和

```sql
CREATE TABLE warehouse
(
    `item` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory

INSERT INTO warehouse VALUES
    ('sku38', '2020-01-01', 9),
    ('sku38', '2020-02-01', 1),
    ('sku38', '2020-03-01', -4),
    ('sku1', '2020-01-01', 1),
    ('sku1', '2020-02-01', 1),
    ('sku1', '2020-03-01', 1);
```    

```sql
SELECT
    item,
    ts,
    value,
    sum(value) OVER (PARTITION BY item ORDER BY ts ASC) AS stock_balance
FROM warehouse
ORDER BY
    item ASC,
    ts ASC;

┌─item──┬──────────────────ts─┬─value─┬─stock_balance─┐
│ sku1  │ 2020-01-01 00:00:00 │     1 │             1 │
│ sku1  │ 2020-02-01 00:00:00 │     1 │             2 │
│ sku1  │ 2020-03-01 00:00:00 │     1 │             3 │
│ sku38 │ 2020-01-01 00:00:00 │     9 │             9 │
│ sku38 │ 2020-02-01 00:00:00 │     1 │            10 │
│ sku38 │ 2020-03-01 00:00:00 │    -4 │             6 │
└───────┴─────────────────────┴───────┴───────────────┘
```

### 移動・スライディング平均 (3行ごと)

```sql
CREATE TABLE sensors
(
    `metric` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory;

insert into sensors values('cpu_temp', '2020-01-01 00:00:00', 87),
                          ('cpu_temp', '2020-01-01 00:00:01', 77),
                          ('cpu_temp', '2020-01-01 00:00:02', 93),
                          ('cpu_temp', '2020-01-01 00:00:03', 87),
                          ('cpu_temp', '2020-01-01 00:00:04', 87),
                          ('cpu_temp', '2020-01-01 00:00:05', 87),
                          ('cpu_temp', '2020-01-01 00:00:06', 87),
                          ('cpu_temp', '2020-01-01 00:00:07', 87);
```

```sql
SELECT
    metric,
    ts,
    value,
    avg(value) OVER (
        PARTITION BY metric 
        ORDER BY ts ASC 
        Rows BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;

┌─metric───┬──────────────────ts─┬─value─┬───moving_avg_temp─┐
│ cpu_temp │ 2020-01-01 00:00:00 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:01 │    77 │                82 │
│ cpu_temp │ 2020-01-01 00:00:02 │    93 │ 85.66666666666667 │
│ cpu_temp │ 2020-01-01 00:00:03 │    87 │ 85.66666666666667 │
│ cpu_temp │ 2020-01-01 00:00:04 │    87 │                89 │
│ cpu_temp │ 2020-01-01 00:00:05 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:06 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:07 │    87 │                87 │
└──────────┴─────────────────────┴───────┴───────────────────┘
```

### 移動・スライディング平均 (10秒ごと)

```sql
SELECT
    metric,
    ts,
    value,
    avg(value) OVER (PARTITION BY metric ORDER BY ts
      Range BETWEEN 10 PRECEDING AND CURRENT ROW) AS moving_avg_10_seconds_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;
    
┌─metric───┬──────────────────ts─┬─value─┬─moving_avg_10_seconds_temp─┐
│ cpu_temp │ 2020-01-01 00:00:00 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:01:10 │    77 │                         77 │
│ cpu_temp │ 2020-01-01 00:02:20 │    93 │                         93 │
│ cpu_temp │ 2020-01-01 00:03:30 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:04:40 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:05:50 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:06:00 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:07:10 │    87 │                         87 │
└──────────┴─────────────────────┴───────┴────────────────────────────┘
```

### 移動・スライディング平均 (10日ごと)

温度は秒単位で保存されますが、`Range`と`ORDER BY toDate(ts)`を使用することで、10単位のサイズのフレームを形成します。`toDate(ts)`を使用しているため、単位は日です。

```sql
CREATE TABLE sensors
(
    `metric` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory;

insert into sensors values('ambient_temp', '2020-01-01 00:00:00', 16),
                          ('ambient_temp', '2020-01-01 12:00:00', 16),
                          ('ambient_temp', '2020-01-02 11:00:00', 9),
                          ('ambient_temp', '2020-01-02 12:00:00', 9),                          
                          ('ambient_temp', '2020-02-01 10:00:00', 10),
                          ('ambient_temp', '2020-02-01 12:00:00', 10),
                          ('ambient_temp', '2020-02-10 12:00:00', 12),                          
                          ('ambient_temp', '2020-02-10 13:00:00', 12),
                          ('ambient_temp', '2020-02-20 12:00:01', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16);
```

```sql
SELECT
    metric,
    ts,
    value,
    round(avg(value) OVER (PARTITION BY metric ORDER BY toDate(ts) 
       Range BETWEEN 10 PRECEDING AND CURRENT ROW),2) AS moving_avg_10_days_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;

┌─metric───────┬──────────────────ts─┬─value─┬─moving_avg_10_days_temp─┐
│ ambient_temp │ 2020-01-01 00:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-01-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-01-02 11:00:00 │     9 │                    12.5 │
│ ambient_temp │ 2020-01-02 12:00:00 │     9 │                    12.5 │
│ ambient_temp │ 2020-02-01 10:00:00 │    10 │                      10 │
│ ambient_temp │ 2020-02-01 12:00:00 │    10 │                      10 │
│ ambient_temp │ 2020-02-10 12:00:00 │    12 │                      11 │
│ ambient_temp │ 2020-02-10 13:00:00 │    12 │                      11 │
│ ambient_temp │ 2020-02-20 12:00:01 │    16 │                   13.33 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
└──────────────┴─────────────────────┴───────┴─────────────────────────┘
```

## 参考文献

### GitHub Issues

ウィンドウ関数の初期サポートのロードマップは[この問題](https://github.com/ClickHouse/ClickHouse/issues/18097)にあります。

ウィンドウ関数に関連するすべてのGitHubの問題は[comp-window-functions](https://github.com/ClickHouse/ClickHouse/labels/comp-window-functions)タグが付いています。

### テスト

これらのテストは現在サポートされている文法の例を含んでいます:

https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/window_functions.xml

https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01591_window_functions.sql

### Postgres Docs

https://www.postgresql.org/docs/current/sql-select.html#SQL-WINDOW

https://www.postgresql.org/docs/devel/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS

https://www.postgresql.org/docs/devel/functions-window.html

https://www.postgresql.org/docs/devel/tutorial-window.html

### MySQL Docs

https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html

https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html

https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html


## 関連コンテンツ

- ブログ: [ClickHouseでの時系列データの操作](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- ブログ: [Gitコミットシーケンスのためのウィンドウと配列関数](https://clickhouse.com/blog/clickhouse-window-array-functions-git-commits)
- ブログ: [ClickHouseへのデータインポート - Part 3 - S3の使用](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)
