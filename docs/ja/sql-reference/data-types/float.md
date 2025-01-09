---
slug: /ja/sql-reference/data-types/float
sidebar_position: 4
sidebar_label: Float32, Float64
---

# Float32, Float64

:::note
高精度が求められる計算、特に金融やビジネスデータの処理には、[Decimal](../data-types/decimal.md) の使用を検討してください。

[浮動小数点数](https://en.wikipedia.org/wiki/IEEE_754)は以下の例のように不正確な結果を招くことがあります：

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
Engine=MergeTree
ORDER BY tuple();

# 1 000 000 個の小数点以下 2 桁のランダムな数値を生成し、float と decimal として格納
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```
```
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```
:::

ClickHouseとCでの対応する型は以下の通りです：

- `Float32` — `float`
- `Float64` — `double`

ClickHouseにおけるFloat型のエイリアスは以下の通りです：

- `Float32` — `FLOAT`, `REAL`, `SINGLE`
- `Float64` — `DOUBLE`, `DOUBLE PRECISION`

テーブル作成時に、浮動小数点数の数値パラメータを設定することができます（例：`FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`）が、ClickHouseはそれらを無視します。

## 浮動小数点数の使用

- 浮動小数点数を利用した計算は、丸め誤差を生じる可能性があります。

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

- 計算結果は計算方法（プロセッサタイプやコンピュータシステムのアーキテクチャ）に依存します。
- 浮動小数点計算の結果として、無限大（`Inf`）や「数ではない」（`NaN`）といった数値を取得することがあります。これを計算結果の処理時に考慮する必要があります。
- テキストから浮動小数点数をパースする際、結果が最も近い機械表現可能な数にならないことがあります。

## NaNとInf

標準SQLとは異なり、ClickHouseは以下の浮動小数点数のカテゴリーをサポートしています：

- `Inf` – 無限大

<!-- -->

``` sql
SELECT 0.5 / 0
```

``` text
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

- `-Inf` — 負の無限大

<!-- -->

``` sql
SELECT -0.5 / 0
```

``` text
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

- `NaN` — 数ではない

<!-- -->

``` sql
SELECT 0 / 0
```

``` text
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

`NaN`のソート順については、[ORDER BY句](../../sql-reference/statements/select/order-by.md)のセクションを参照してください。
