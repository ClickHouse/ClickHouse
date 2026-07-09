---
description: '精度を設定できる固定小数点演算を提供する、ClickHouse の Decimal データ型に関するドキュメント'
sidebar_label: 'Decimal'
sidebar_position: 6
slug: /sql-reference/data-types/decimal
title: 'Decimal, Decimal(P), Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S),
  Decimal256(S)'
doc_type: 'reference'
---

加算、減算、乗算では精度が維持される符号付き固定小数点数です。除算では下位桁は切り捨てられます (丸めは行われません) 。

<div id="parameters">
  ## パラメータ
</div>

* P - 精度。有効範囲: [ 1 : 76 ]。数値全体で使用できる10進桁数 (小数部を含む) を指定します。既定値は 10 です。
* S - スケール。有効範囲: [ 0 : P ]。小数部で使用できる10進桁数を指定します。

Decimal(P) は Decimal(P, 0) と同等です。同様に、構文 Decimal は Decimal(10, 0) と同等です。

P パラメータの値に応じて、Decimal(P, S) は次の型の別名になります。

* P が [ 1 : 9 ] の場合 - Decimal32(S)
* P が [ 10 : 18 ] の場合 - Decimal64(S)
* P が [ 19 : 38 ] の場合 - Decimal128(S)
* P が [ 39 : 76 ] の場合 - Decimal256(S)

<div id="decimal-value-ranges">
  ## Decimal の値の範囲
</div>

* Decimal(P, S) - ( -1 * 10^(P - S), 1 * 10^(P - S) )
* Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
* Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
* Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )
* Decimal256(S) - ( -1 * 10^(76 - S), 1 * 10^(76 - S) )

たとえば、Decimal32(4) には、-99999.9999 から 99999.9999 までの数値を 0.0001 刻みで格納できます。

<div id="internal-representation">
  ## 内部表現
</div>

内部的には、データはそれぞれのビット幅に応じた通常の符号付き整数として表現されます。メモリに格納できる実際の値の範囲は、上で示した値よりやや広くなりますが、これがチェックされるのは文字列からの変換時のみです。

現在のCPUは128ビットおよび256ビット整数をネイティブにサポートしていないため、Decimal128 と Decimal256 の操作はエミュレーションによって実行されます。したがって、Decimal128 と Decimal256 は Decimal32/Decimal64 と比べて大幅に低速です。

<div id="operations-and-result-type">
  ## 操作と結果型
</div>

Decimal に対する二項演算の結果型は、引数の順序にかかわらず、より広い型になります。

* `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
* `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
* `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`
* `Decimal256(S1) <op> Decimal<32|64|128>(S2) -> Decimal256(S)`

スケールに関する規則:

* add, subtract: S = max(S1, S2).
* multiply: S = S1 + S2.
* divide: S = S1.

Decimal と整数の間で同様の演算を行う場合、結果は引数と同じサイズの Decimal になります。

Decimal と Float32/Float64 の間の演算は定義されていません。必要な場合は、toDecimal32、toDecimal64、toDecimal128、または toFloat32、toFloat64 の組み込み関数を使って、片方の引数を明示的にキャストできます。ただし、結果の精度は失われ、型変換には計算コストがかかることに注意してください。

Decimal に対する一部の関数は、結果を Float64 として返します (たとえば、var や stddev) 。中間計算は引き続き Decimal で実行されることがあるため、同じ値を持つ Float64 入力と Decimal 入力とで結果が異なる場合があります。

<div id="overflow-checks">
  ## オーバーフローチェック
</div>

Decimal の計算では、整数オーバーフローが発生することがあります。小数部の桁数が多すぎる場合は切り捨てられ (丸めは行われません) 、整数部の桁数が多すぎる場合は例外が発生します。

:::warning
Decimal128 と Decimal256 では、オーバーフローチェックは実装されていません。オーバーフローした場合は誤った結果が返され、例外は発生しません。
:::

```sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

```text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

```sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

```text
DB::Exception: Scale is out of bounds.
```

```sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
DB::Exception: Decimal math overflow.
```

オーバーフローチェックを行うと、処理が遅くなります。オーバーフローが発生しないことが分かっている場合は、`decimal_check_overflow` 設定でチェックを無効にするのが適切です。チェックを無効にした状態でオーバーフローが発生すると、結果は不正確になります。

```sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

オーバーフローチェックは、算術演算だけでなく、値の比較時にも行われます：

```sql
SELECT toDecimal32(1, 8) < 100
```

```text
DB::Exception: Can't compare.
```

**関連項目**

* [isDecimalOverflow](/ja/sql-reference/functions/other-functions#isDecimalOverflow)
* [countDigits](/ja/sql-reference/functions/other-functions#countDigits)