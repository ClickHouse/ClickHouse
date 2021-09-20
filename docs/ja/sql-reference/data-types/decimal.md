---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "\u5C0F\u6570\u70B9"
---

# Decimal(P,S),Decimal32(S),Decimal64(S),Decimal128(S) {#decimalp-s-decimal32s-decimal64s-decimal128s}

加算、減算、および乗算の演算中に精度を維持する符号付き固定小数点数。 除算の場合、最下位桁数は破棄されます（丸められません）。

## パラメータ {#parameters}

-   P-精度。 有効な範囲:\[1:38\]。 小数の桁数を決定します(分数を含む)。
-   Sスケール 有効な範囲:\[0:P\]。 小数の桁数を指定します。

Pに依存するパラメータ値Decimal(P,S)は、:
-P from\[1:9\]-For Decimal32(S)
-P from\[10:18\]-For Decimal64(S)
-P from\[19:38\]-For Decimal128(S)

## 小数点以下の値の範囲 {#decimal-value-ranges}

-   Decimal32(S) - ( -1 \* 10^(9 - S),1\*10^(9-S) )
-   Decimal64(S) - ( -1 \* 10^(18 - S),1\*10^(18-S) )
-   Decimal128(S) - ( -1 \* 10^(38 - S),1\*10^(38-S) )

たとえば、Decimal32(4)には、-99999.9999から99999.9999までの0.0001ステップの数値を含めることができます。

## 内部表現 {#internal-representation}

社内データとして表される通常の署名の整数をそれぞれのビット幅になります。 メモリに格納できる実際の値の範囲は、上記の指定よりも少し大きく、文字列からの変換時にのみチェックされます。

現代のCPUは128ビット整数をネイティブにサポートしていないため、Decimal128の演算はエミュレートされます。 このため、Decimal128はDecimal32/Decimal64よりも大幅に遅く動作します。

## 操作と結果の種類 {#operations-and-result-type}

Decimalのバイナリ演算は、より広い結果の型になります（引数の順序は任意です）。

-   `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
-   `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
-   `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`

スケールのルール:

-   加算、減算：S=max（S1、S2）。
-   multuply:S=S1+S2.
-   除算:S=S1.

Decimalと整数の間の同様の演算の場合、結果は引数と同じサイズのDecimalになります。

DecimalとFloat32/Float64の間の演算は定義されていません。 必要な場合は、toDecimal32、toDecimal64、toDecimal128、またはtoFloat32、toFloat64ビルトインを使用して引数のいずれかを明示的にキャストできます。 結果は精度が失われ、型変換は計算コストがかかる操作であることに注意してください。

Decimalの一部の関数はFloat64(varやstddevなど)として結果を返します。 これにより、Float64入力とDecimal入力が同じ値で異なる結果になる可能性があります。

## オーバーフローチェック {#overflow-checks}

中計算は小数,整数であふれかが起こる。 小数部の過剰な数字は破棄されます（丸められません）。 整数部分の数字が過剰になると、例外が発生します。

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

オーバーフローチェックが業務に減速した。 が知られていることができませんこを無効にするチェック `decimal_check_overflow` 設定。 時チェックを無効とオーバーフローが起こり、結果は正しくあり:

``` sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

オーバーフローチェックが起こるだけでなく演算業務はもとより、価値との比較:

``` sql
SELECT toDecimal32(1, 8) < 100
```

``` text
DB::Exception: Can't compare.
```

[元の記事](https://clickhouse.com/docs/en/data_types/decimal/) <!--hide-->
