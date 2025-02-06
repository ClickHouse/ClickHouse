---
slug: /ja/sql-reference/data-types/decimal
sidebar_position: 6
sidebar_label: Decimal
---

# Decimal, Decimal(P), Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)

符号付きの固定小数点数で、加算、減算、乗算の際に精度を保持します。除算では、小数部分の最下位桁が切り捨てられます（四捨五入しません）。

## パラメータ

- P - 精度。 有効範囲: \[ 1 : 76 \]。数値が持つことができる10進数の桁数（小数部を含む）を決定します。デフォルトでは、精度は10です。
- S - スケール。 有効範囲: \[ 0 : P \]。小数部が持つことができる10進数の桁数を決定します。

Decimal(P)はDecimal(P, 0)と同等です。同様に、Decimalという構文はDecimal(10, 0)と同等です。

Pパラメータ値に応じて、Decimal(P, S)は以下の代替表記があります:
- Pが \[ 1 : 9 \] の場合 - Decimal32(S)
- Pが \[ 10 : 18 \] の場合 - Decimal64(S)
- Pが \[ 19 : 38 \] の場合 - Decimal128(S)
- Pが \[ 39 : 76 \] の場合 - Decimal256(S)

## Decimal 値の範囲

- Decimal32(S) - ( -1 \* 10^(9 - S), 1 \* 10^(9 - S) )
- Decimal64(S) - ( -1 \* 10^(18 - S), 1 \* 10^(18 - S) )
- Decimal128(S) - ( -1 \* 10^(38 - S), 1 \* 10^(38 - S) )
- Decimal256(S) - ( -1 \* 10^(76 - S), 1 \* 10^(76 - S) )

例えば、Decimal32(4) は -99999.9999 から 99999.9999 までの数値を0.0001単位で含むことができます。

## 内部表現

内部的には、データはそれぞれのビット幅を持つ通常の符号付き整数として表現されます。メモリに格納可能な実際の値の範囲は上記より少し大きく、文字列からの変換時にのみチェックされます。

現代のCPUは128ビットと256ビットの整数をネイティブにサポートしていないため、Decimal128およびDecimal256の操作はエミュレートされます。したがって、Decimal128およびDecimal256はDecimal32/Decimal64よりもかなり遅く動作します。

## 演算と結果の型

Decimalに対する二項演算は、より幅広い結果型となります（引数の順序にかかわらず）。

- `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
- `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
- `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`
- `Decimal256(S1) <op> Decimal<32|64|128>(S2) -> Decimal256(S)`

スケールのルール:

- 加算、減算: S = max(S1, S2)。
- 乗算: S = S1 + S2。
- 除算: S = S1。

Decimalと整数の間での類似演算の場合、結果は引数と同じサイズのDecimalになります。

DecimalとFloat32/Float64の間の演算は定義されていません。それらが必要な場合は、toDecimal32、toDecimal64、toDecimal128 あるいは toFloat32、toFloat64という組み込み関数でどちらかの引数を明示的にキャストすることができます。ただし、結果は精度を失い、型変換は計算コストの高い操作であることに留意してください。

一部のDecimal関数の結果はFloat64として返されます（例えば、varやstddev）。中間計算は依然としてDecimalで実行される可能性があり、Float64と同じ値でDecimal入力を行った場合に異なる結果が生成されることがあります。

## オーバーフローチェック

Decimalの計算中に整数オーバーフローが発生する可能性があります。小数部の過剰な桁は切り捨てられます（四捨五入しません）。整数部の過剰な桁は例外を引き起こします。

:::warning
オーバーフローチェックはDecimal128およびDecimal256に対して実装されていません。オーバーフローが発生した場合、不正確な結果が返され、例外は投げられません。
:::

``` sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

``` text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

``` text
DB::Exception: Scale is out of bounds.
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
DB::Exception: Decimal math overflow.
```

オーバーフローチェックは演算の速度を遅くします。オーバーフローが発生しないことがわかっている場合は、`decimal_check_overflow`設定を使用してチェックを無効にすることをお勧めします。チェックを無効にし、オーバーフローが発生すると、結果は不正確になります。

``` sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

オーバーフローチェックは算術演算だけでなく、値の比較でも発生します。

``` sql
SELECT toDecimal32(1, 8) < 100
```

``` text
DB::Exception: Can't compare.
```

**関連項目**
- [isDecimalOverflow](../../sql-reference/functions/other-functions.md#is-decimal-overflow)
- [countDigits](../../sql-reference/functions/other-functions.md#count-digits)
