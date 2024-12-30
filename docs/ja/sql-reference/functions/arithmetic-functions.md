---
slug: /ja/sql-reference/functions/arithmetic-functions
sidebar_position: 5
sidebar_label: 算術関数
---

# 算術関数

算術関数は、`UInt8`、`UInt16`、`UInt32`、`UInt64`、`Int8`、`Int16`、`Int32`、`Int64`、`Float32`、もしくは `Float64` 型の2つのオペランドに対応します。

演算を実行する前に、両方のオペランドは結果の型にキャストされます。結果の型は以下のように決定されます（以下の関数のドキュメントで特に指定されていない限り）：
- 両方のオペランドが32ビット以内であれば、結果の型のサイズは、より大きいオペランドに続く次に大きい型のサイズとなります（整数サイズの昇格）。例：`UInt8 + UInt16 = UInt32` または `Float32 * Float32 = Float64`。
- オペランドのうちの1つが64ビット以上であれば、結果の型のサイズは2つのオペランドのうち大きい方のサイズと同じになります。例：`UInt32 + UInt128 = UInt128` または `Float32 * Float64 = Float64`。
- オペランドのうち1つが符号付きの場合、結果の型も符号付きになります。そうでない場合は符号付きになります。例：`UInt32 * Int32 = Int64`。

これらのルールにより、すべての可能な結果を表現できる最小の型が結果の型となることが保証されます。これは値の範囲境界付近でオーバーフローのリスクを伴いますが、64ビットのネイティブ整数幅を使用して計算が迅速に行われることを保証します。この挙動は、最大整数型として64ビット整数（BIGINT）を提供する多くの他のデータベースとの互換性も保証します。

例：

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

オーバーフローはC++と同様に生成されます。

## plus

2つの値 `a` と `b` の合計を計算します。

**構文**

```sql
plus(a, b)
```

整数と日付または時間付き日付を加算することも可能です。前者の操作は日付の日数を増加させ、後者の操作は時間付き日付の秒数を増加させます。

エイリアス: `a + b` (演算子)

## minus

2つの値 `a` と `b` の差を計算します。結果は常に符号付きです。

`plus` と同様に、整数から日付または時間付き日付を引くことが可能です。

**構文**

```sql
minus(a, b)
```

エイリアス: `a - b` (演算子)

## multiply

2つの値 `a` と `b` の積を計算します。

**構文**

```sql
multiply(a, b)
```

エイリアス: `a * b` (演算子)

## divide

2つの値 `a` と `b` の商を計算します。結果の型は常に [Float64](../data-types/float.md) です。整数除算は `intDiv` 関数で提供されます。

ゼロで割ると `inf`、`-inf`、または `nan` を返します。

**構文**

```sql
divide(a, b)
```

エイリアス: `a / b` (演算子)

## intDiv

2つの値 `a` と `b` の整数除算を行います。すなわち、商を次に小さい整数に切り下げます。

結果は被除数（最初のパラメータ）と同じ幅です。

ゼロで割るとき、商が被除数の範囲に収まらないとき、または最小の負の数を-1で割るときは例外が発生します。

**構文**

```sql
intDiv(a, b)
```

**例**

クエリ：

```sql
SELECT
    intDiv(toFloat64(1), 0.001) AS res,
    toTypeName(res)
```

```response
┌──res─┬─toTypeName(intDiv(toFloat64(1), 0.001))─┐
│ 1000 │ Int64                                   │
└──────┴─────────────────────────────────────────┘
```

```sql
SELECT
    intDiv(1, 0.001) AS res,
    toTypeName(res)
```

```response
Received exception from server (version 23.2.1):
Code: 153. DB::Exception: Received from localhost:9000. DB::Exception: Cannot perform integer division, because it will produce infinite or too large number: While processing intDiv(1, 0.001) AS res, toTypeName(res). (ILLEGAL_DIVISION)
```

## intDivOrZero

`intDiv` と同様ですが、ゼロで割る場合や最小の負の数を-1で割る場合はゼロを返します。

**構文**

```sql
intDivOrZero(a, b)
```

## isFinite

Float32 または Float64 引数が無限でなく NaN でもない場合に1を返します。それ以外の場合、この関数は0を返します。

**構文**

```sql
isFinite(x)
```

## isInfinite

Float32 または Float64 引数が無限である場合に1を返します。それ以外の場合、この関数は0を返します。NaN に対しては0が返されることに注意してください。

**構文**

```sql
isInfinite(x)
```

## ifNotFinite

浮動小数点の値が有限かどうかをチェックします。

**構文**

```sql
ifNotFinite(x,y)
```

**引数**

- `x` — 無限かどうかをチェックする値。[Float\*](../data-types/float.md)。
- `y` — フォールバック値。[Float\*](../data-types/float.md)。

**返される値**

- `x` が有限なら `x`。
- `x` が有限でなければ `y`。

**例**

クエリ：

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

結果：

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

同様の結果を[三項演算子](../../sql-reference/functions/conditional-functions.md#ternary-operator)を使用して得ることができます: `isFinite(x) ? x : y`。

## isNaN

Float32 と Float64 引数が NaN の場合に1を返し、それ以外の場合には0を返します。

**構文**

```sql
isNaN(x)
```

## modulo

2つの値 `a` と `b` を割った余りを計算します。

両方の入力が整数であれば、結果の型は整数です。入力の片方が浮動小数点数であれば、結果の型は [Float64](../data-types/float.md) です。

余りは C++ と同様に計算されます。負の数の場合、切り捨て除算が使用されます。

ゼロで割る場合や最小の負の数を-1で割る場合は例外が発生します。

**構文**

```sql
modulo(a, b)
```

エイリアス: `a % b` (演算子)

## moduloOrZero

[modulo](#modulo) と同様ですが、除数がゼロの場合にはゼロを返します。

**構文**

```sql
moduloOrZero(a, b)
```

## positiveModulo(a, b)

[modulo](#modulo) と同様ですが、常に非負の数を返します。

この関数は `modulo` より4-5倍遅くなります。

**構文**

```sql
positiveModulo(a, b)
```

エイリアス:
- `positive_modulo(a, b)`
- `pmod(a, b)`

**例**

クエリ：

```sql
SELECT positiveModulo(-1, 10)
```

結果：

```result
┌─positiveModulo(-1, 10)─┐
│                      9 │
└────────────────────────┘
```

## negate

値 `a` を反転します。結果は常に符号付きです。

**構文**

```sql
negate(a)
```

エイリアス: `-a`

## abs

`a` の絶対値を計算します。`a` が符号なし型の場合は何の効果もありません。`a` が符号付き型である場合、符号なし数を返します。

**構文**

```sql
abs(a)
```

## gcd

2つの値 `a` と `b` の最大公約数を返します。

ゼロで割る場合や最小の負の数を-1で割る場合は例外が発生します。

**構文**

```sql
gcd(a, b)
```

## lcm(a, b)

2つの値 `a` と `b` の最小公倍数を返します。

ゼロで割る場合や最小の負の数を-1で割る場合は例外が発生します。

**構文**

```sql
lcm(a, b)
```

## max2

2つの値 `a` と `b` のうち大きい方を返します。返される値の型は [Float64](../data-types/float.md) です。

**構文**

```sql
max2(a, b)
```

**例**

クエリ：

```sql
SELECT max2(-1, 2);
```

結果：

```result
┌─max2(-1, 2)─┐
│           2 │
└─────────────┘
```

## min2

2つの値 `a` と `b` のうち小さい方を返します。返される値の型は [Float64](../data-types/float.md) です。

**構文**

```sql
min2(a, b)
```

**例**

クエリ：

```sql
SELECT min2(-1, 2);
```

結果：

```result
┌─min2(-1, 2)─┐
│          -1 │
└─────────────┘
```

## multiplyDecimal

2つの10進数 `a` と `b` を掛け算します。結果の値の型は [Decimal256](../data-types/decimal.md) です。

結果のスケールは `result_scale` によって明示的に指定できます。`result_scale` が指定されない場合、入力値の最大スケールが仮定されます。

この関数は通常の `multiply` よりも大幅に遅いです。結果の精度に対する制御が必要ない場合や高速な計算が望ましい場合は、`multiply` の使用を検討してください。

**構文**

```sql
multiplyDecimal(a, b[, result_scale])
```

**引数**

- `a` — 最初の値。[Decimal](../data-types/decimal.md)。
- `b` — 2番目の値。[Decimal](../data-types/decimal.md)。
- `result_scale` — 結果のスケール。[Int/UInt](../data-types/int-uint.md)。

**返される値**

- 与えられたスケールでの乗算の結果。[Decimal256](../data-types/decimal.md)。

**例**

```result
┌─multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1)─┐
│                                                           25.2 │
└────────────────────────────────────────────────────────────────┘
```

**通常の乗算に比べた違い：**

```sql
SELECT toDecimal64(-12.647, 3) * toDecimal32(2.1239, 4);
SELECT toDecimal64(-12.647, 3) as a, toDecimal32(2.1239, 4) as b, multiplyDecimal(a, b);
```

結果：

```result
┌─multiply(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                               -26.8609633 │
└───────────────────────────────────────────────────────────┘
┌───────a─┬──────b─┬─multiplyDecimal(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│ -12.647 │ 2.1239 │                                                         -26.8609 │
└─────────┴────────┴──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    multiplyDecimal(a, b);

SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    a * b;
```

結果：

```result
┌─────────────a─┬─────────────b─┬─multiplyDecimal(toDecimal64(-12.647987876, 9), toDecimal64(123.967645643, 9))─┐
│ -12.647987876 │ 123.967645643 │                                                               -1567.941279108 │
└───────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘

Received exception from server (version 22.11.1):
Code: 407. DB::Exception: Received from localhost:9000. DB::Exception: Decimal math overflow: While processing toDecimal64(-12.647987876, 9) AS a, toDecimal64(123.967645643, 9) AS b, a * b. (DECIMAL_OVERFLOW)
```

## divideDecimal

2つの10進数 `a` と `b` を除算します。結果の値の型は [Decimal256](../data-types/decimal.md) です。

結果のスケールは `result_scale` によって明示的に指定できます。`result_scale` が指定されない場合、入力値の最大スケールが仮定されます。

この関数は通常の `divide` よりも大幅に遅いです。結果の精度に対する制御が必要ない場合や高速な計算が望ましい場合は、`divide` の使用を検討してください。

**構文**

```sql
divideDecimal(a, b[, result_scale])
```

**引数**

- `a` — 最初の値：[Decimal](../data-types/decimal.md)。
- `b` — 2番目の値：[Decimal](../data-types/decimal.md)。
- `result_scale` — 結果のスケール：[Int/UInt](../data-types/int-uint.md)。

**返される値**

- 与えられたスケールでの除算の結果。[Decimal256](../data-types/decimal.md)。

**例**

```result
┌─divideDecimal(toDecimal256(-12, 0), toDecimal32(2.1, 1), 10)─┐
│                                                -5.7142857142 │
└──────────────────────────────────────────────────────────────┘
```

**通常の除算に比べた違い：**

```sql
SELECT toDecimal64(-12, 1) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 1) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

結果：

```result
┌─divide(toDecimal64(-12, 1), toDecimal32(2.1, 1))─┐
│                                             -5.7 │
└──────────────────────────────────────────────────┘

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 1), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 1), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal64(-12, 0) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 0) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

結果：

```result
DB::Exception: Decimal result's scale is less than argument's one: While processing toDecimal64(-12, 0) / toDecimal32(2.1, 1). (ARGUMENT_OUT_OF_BOUND)

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

## byteSwap

整数のバイトを反転します。すなわち、その[エンディアン](https://en.wikipedia.org/wiki/Endianness)を変更します。

**構文**

```sql
byteSwap(a)
```

**例**

```sql
byteSwap(3351772109)
```

結果：

```result
┌─byteSwap(3351772109)─┐
│           3455829959 │
└──────────────────────┘
```

上記の例は次のようにして導き出せます：
1. 10進数の整数をビッグエンディアン形式の16進数形式に変換します。例: 3351772109 -> C7 C7 FB CD (4バイト)
2. バイトを反転します。例: C7 C7 FB CD -> CD FB C7 C7
3. 結果をビッグエンディアンを仮定して整数に変換します。例: CD FB C7 C7  -> 3455829959

この関数の一つのユースケースはIPv4の反転です：

```result
┌─toIPv4(byteSwap(toUInt32(toIPv4('205.251.199.199'))))─┐
│ 199.199.251.205                                       │
└───────────────────────────────────────────────────────┘
```
