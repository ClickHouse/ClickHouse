---
slug: /ja/sql-reference/functions/rounding-functions
sidebar_position: 155
sidebar_label: 丸め
---

# 丸め関数

## floor

`x` 以下の最大の切り捨て数値を返します。切り捨て数値は 1 / 10 * N の倍数であり、1 / 10 * N が正確でない場合、適切なデータ型に最も近い数値です。

整数の引数は負の `N` 引数で丸められ、非負の `N` の場合、関数は `x` を返します。つまり、何もしません。

丸めによりオーバーフローが発生した場合（例: `floor(-128, -1)`）、結果は未定義です。

**構文**

``` sql
floor(x[, N])
```

**パラメーター**

- `x` - 丸める値。[Float*](../data-types/float.md)、[Decimal*](../data-types/decimal.md)、または [(U)Int*](../data-types/int-uint.md)。
- `N` . [(U)Int*](../data-types/int-uint.md)。デフォルトはゼロで、整数への丸めを意味します。負にすることも可能です。

**返される値**

`x` と同じ型の切り捨て数値。

**例**

クエリ:

```sql
SELECT floor(123.45, 1) AS rounded
```

結果:

```
┌─rounded─┐
│   123.4 │
└─────────┘
```

クエリ:

```sql
SELECT floor(123.45, -1)
```

結果:

```
┌─rounded─┐
│     120 │
└─────────┘
```

## ceiling

`floor` と似ていますが、`x` 以上の最小の切り上げ数値を返します。

**構文**

``` sql
ceiling(x[, N])
```

別名: `ceil`

## truncate

`floor` と似ていますが、`x` の絶対値以下の最大絶対値を持つ切り捨て数値を返します。

**構文**

```sql
truncate(x[, N])
```

別名: `trunc`.

**例**

クエリ:

```sql
SELECT truncate(123.499, 1) as res;
```

```response
┌───res─┐
│ 123.4 │
└───────┘
```

## round

指定された小数点以下の桁数に値を丸めます。

この関数は、指定されたオーダーの最も近い数値を返します。入力値が隣接する2つの数値に等しい距離の場合、この関数は [Float*](../data-types/float.md) 入力に対してはバンカーズ・ラウンディングを使用し、他の型（[Decimal*](../data-types/decimal.md)）に対しては零から遠ざけて丸めます。

**構文**

``` sql
round(x[, N])
```

**引数**

- `x` — 丸める数値。[Float*](../data-types/float.md)、[Decimal*](../data-types/decimal.md)、または [(U)Int*](../data-types/int-uint.md)。
- `N` — 丸める小数点以下の桁数。整数。デフォルトは `0`。
    - `N > 0` の場合、関数は小数点以下を丸めます。
    - `N < 0` の場合、関数は小数点の左を丸めます。
    - `N = 0` の場合、関数は次の整数に丸めます。

**返される値:**

`x` と同じ型の切り捨て数値。

**例**

`Float` 入力の場合の例:

```sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3;
```

```
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

`Decimal` 入力の場合の例:

```sql
SELECT cast(number / 2 AS  Decimal(10,4)) AS x, round(x) FROM system.numbers LIMIT 3;
```

```
┌───x─┬─round(CAST(divide(number, 2), 'Decimal(10, 4)'))─┐
│   0 │                                                0 │
│ 0.5 │                                                1 │
│   1 │                                                1 │
└─────┴──────────────────────────────────────────────────┘
```

末尾のゼロを保持するには、設定 `output_format_decimal_trailing_zeros` を有効にします:

```sql
SELECT cast(number / 2 AS  Decimal(10,4)) AS x, round(x) FROM system.numbers LIMIT 3 settings output_format_decimal_trailing_zeros=1;

```

```
┌──────x─┬─round(CAST(divide(number, 2), 'Decimal(10, 4)'))─┐
│ 0.0000 │                                           0.0000 │
│ 0.5000 │                                           1.0000 │
│ 1.0000 │                                           1.0000 │
└────────┴──────────────────────────────────────────────────┘
```

最も近い数値への丸めの例:

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

バンカーズ・ラウンディング。

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**関連項目**

- [roundBankers](#roundbankers)

## roundBankers

指定された小数位置に数値を丸めます。

丸める数値が2つの数値の中間にある場合、この関数はバンカーズ・ラウンディングを使用します。バンカーズ・ラウンディングは、数値を丸める方法で、数値が中間にある場合、指定された小数位置で最も近い偶数に丸められます。例えば: 3.5は4に丸められ、2.5は2に丸められます。これは、[IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest)で定義された浮動小数点数のデフォルトの丸め方法です。[round](#round) 関数も同じ丸めを浮動小数点数に対して行います。`roundBankers` 関数も整数を同じ方法で丸めます。例えば、 `roundBankers(45, -1) = 40`。

他のケースでは、関数は数値を最も近い整数に丸めます。

バンカーズ・ラウンディングを使用することで、この数値を合計または減算する結果に対する丸め数値の影響を減少させることができます。

例えば、1.5、2.5、3.5、4.5を異なる丸めで合計します:

- 丸めなし: 1.5 + 2.5 + 3.5 + 4.5 = 12.
- バンカーズ・ラウンディング: 2 + 2 + 4 + 4 = 12.
- 最も近い整数への丸め: 2 + 3 + 4 + 5 = 14.

**構文**

``` sql
roundBankers(x [, N])
```

**引数**

    - `N > 0` — 関数は小数点以下の右側にある指定された位置に数値を丸めます。例: `roundBankers(3.55, 1) = 3.6`。
    - `N < 0` — 関数は小数点左側の指定された位置に数値を丸めます。例: `roundBankers(24.55, -1) = 20`。
    - `N = 0` — 関数は数値を整数に丸めます。この場合、引数は省略可能です。例: `roundBankers(2.5) = 2`。

- `x` — 丸める数値。[Float*](../data-types/float.md)、[Decimal*](../data-types/decimal.md)、または [(U)Int*](../data-types/int-uint.md)。
- `N` — 丸める小数点以下の桁数。整数。デフォルトは `0`。
    - `N > 0` の場合、関数は小数点以下を丸めます。
    - `N < 0` の場合、関数は小数点の左を丸めます。
    - `N = 0` の場合、関数は次の整数に丸めます。

**返される値**

バンカーズ・ラウンディング法で丸めれた値。

**例**

クエリ:

```sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

結果:

```
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

バンカーズ・ラウンディングの例:

```
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 10.76
```

**関連項目**

- [round](#round)

## roundToExp2

数値を受け取り、その数値が1未満の場合、`0` を返します。それ以外の場合は、最も近い2の指数に丸めます。

**構文**

```sql
roundToExp2(num)
```

**パラメーター**

- `num`: 丸める数値。[UInt](../data-types/int-uint.md)/[Float](../data-types/float.md)。

**返される値**

- `num` が1未満の場合 `0`。[UInt8](../data-types/int-uint.md)。
- `num` を最も近い2の指数に丸めます。[UInt](../data-types/int-uint.md)/[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT *, roundToExp2(*) FROM system.numbers WHERE number IN (0, 2, 5, 10, 19, 50)
```

結果:

```response
┌─number─┬─roundToExp2(number)─┐
│      0 │                   0 │
│      2 │                   2 │
│      5 │                   4 │
│     10 │                   8 │
│     19 │                  16 │
│     50 │                  32 │
└────────┴─────────────────────┘
```

## roundDuration

数値を受け取り、その数値が1未満の場合、`0` を返します。それ以外の場合は、よく使われる期間のセットから最も近い数値に丸めます: `1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000`。

**構文**

```sql
roundDuration(num)
```

**パラメーター**

- `num`: よく使われる期間セットの一つに丸める数値。[UInt](../data-types/int-uint.md)/[Float](../data-types/float.md)。

**返される値**

- `num` が1未満の場合 `0`。
- それ以外の場合: `1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000` の一つ。[UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT *, roundDuration(*) FROM system.numbers WHERE number IN (0, 9, 19, 47, 101, 149, 205, 271, 421, 789, 1423, 2345, 4567, 9876, 24680, 42573)
```

結果:

```response
┌─number─┬─roundDuration(number)─┐
│      0 │                     0 │
│      9 │                     1 │
│     19 │                    10 │
│     47 │                    30 │
│    101 │                    60 │
│    149 │                   120 │
│    205 │                   180 │
│    271 │                   240 │
│    421 │                   300 │
│    789 │                   600 │
│   1423 │                  1200 │
│   2345 │                  1800 │
│   4567 │                  3600 │
│   9876 │                  7200 │
│  24680 │                 18000 │
│  42573 │                 36000 │
└────────┴───────────────────────┘
```

## roundAge

さまざまな一般的な人間の年齢範囲内で数値を受取り、その範囲内の最大または最小値を返します。

**構文**

```sql
roundAge(num)
```

**パラメーター**

- `age`: 年齢を表す数値（年単位）。[UInt](../data-types/int-uint.md)/[Float](../data-types/float.md)。

**返される値**

- `age` が1未満の場合 `0` を返します。
- `1 <= age <= 17` の場合 `17` を返します。
- `18 <= age <= 24` の場合 `18` を返します。
- `25 <= age <= 34` の場合 `25` を返します。
- `35 <= age <= 44` の場合 `35` を返します。
- `45 <= age <= 54` の場合 `45` を返します。
- `age >= 55` の場合 `55` を返します。

型: [UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT *, roundAge(*) FROM system.numbers WHERE number IN (0, 5, 20, 31, 37, 54, 72);
```

結果:

```response
┌─number─┬─roundAge(number)─┐
│      0 │                0 │
│      5 │               17 │
│     20 │               18 │
│     31 │               25 │
│     37 │               35 │
│     54 │               45 │
│     72 │               55 │
└────────┴──────────────────┘
```

## roundDown

数値を受け取り、指定された配列内の要素に対して切り捨てを行います。値が最も低い境界よりも小さい場合、最も低い境界が返されます。

**構文**

```sql
roundDown(num, arr)
```

**パラメーター**

- `num`: 切り捨てする数値。[Numeric](../data-types/int-uint.md)。
- `arr`: `age` を切り捨てするための要素の配列。[Array](../data-types/array.md)の[UInt](../data-types/int-uint.md)/[Float](../data-types/float.md)型。

**返される値**

- 配列`arr` の要素に切り捨てられた数値。値が最も低い境界より小さい場合、最も低い境界が返されます。[UInt](../data-types/int-uint.md)/[Float](../data-types/float.md)型は`arr`の型により決定されます。

**例**

クエリ:

```sql
SELECT *, roundDown(*, [3, 4, 5]) FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5)
```

結果:

```response
┌─number─┬─roundDown(number, [3, 4, 5])─┐
│      0 │                            3 │
│      1 │                            3 │
│      2 │                            3 │
│      3 │                            3 │
│      4 │                            4 │
│      5 │                            5 │
└────────┴──────────────────────────────┘
```
