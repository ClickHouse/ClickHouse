---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 42
toc_title: "\u5C0F\u6570"
---

# 小数点(p,s)、小数点32(s)、小数点64(s)、小数点128(s) {#decimalp-s-decimal32s-decimal64s-decimal128s}

加算、減算、および乗算の演算時に精度を維持する符号付き固定小数点数。 除算の場合、最下位の数字は破棄されます（丸められていません）。

## パラメータ {#parameters}

-   P-精度。 有効な範囲:\[1:38\]。 小数点以下の桁数(分数を含む)を指定します。
-   S-スケール。 有効な範囲:\[0:P\]。 小数部の桁数を指定します。

Pパラメータ値に応じてDecimal(P,S)は以下のシノニムです:
-Pから\[1:9\]-用Decimal32(S)
-Pから\[10:18\]-用Decimal64(S)
-Pから\[19:38\]-用Decimal128(S)

## 小数値の範囲 {#decimal-value-ranges}

-   デシマル32(s) - ( -1 \* 10^(9 - s）、1\*10^（9-s) )
-   Decimal64(S) - ( -1 \* 10^(18 - S）、1\*10^（18-S) )
-   Decimal128(S) - ( -1 \* 10^(38 - S）、1\*10^（38-S) )

たとえば、decimal32(4)には、-99999.9999から99999.9999までの0.0001ステップの数値を含めることができます。

## 内部表現 {#internal-representation}

社内データとして表される通常の署名の整数をそれぞれのビット幅になります。 メモリに格納できる実際の値の範囲は、上記で指定した値より少し大きくなり、文字列からの変換でのみチェックされます。

現代のcpuはネイティブに128ビットの整数をサポートしていないため、decimal128の演算はエミュレートされます。 このため、decimal128はdecimal32/decimal64よりも大幅に遅く動作します。

## 操作と結果の種類 {#operations-and-result-type}

Decimalのバイナリ演算では、結果の型が広くなります（引数の順序は任意です）。

-   `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
-   `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
-   `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`

スケールのルール:

-   加算、減算：S=最大（S1、S2）。
-   multuply:S=S1+S2.
-   分割：S=S1。

Decimalと整数の間の同様の演算の場合、結果は引数と同じサイズのDecimalになります。

DecimalとFloat32/Float64の間の演算は定義されていません。 それらが必要な場合は、toDecimal32、toDecimal64、toDecimal128またはtoFloat32、tofat64組み込み関数を使用して明示的に引数をキャストできます。 結果は精度を失い、型変換は計算コストのかかる演算であることに注意してください。

Decimalの一部の関数は、Float64として結果を返します（たとえば、varまたはstddev）。 これは、Float64とDecimal入力の間で同じ値を持つ異なる結果につながる可能性があります。

## オーバーフロ {#overflow-checks}

中計算は小数,整数であふれかが起こる。 小数部の余分な桁は破棄されます（丸められていません）。 整数部分の数字が多すぎると、例外が発生します。

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

オーバーフローチェックが業務に減速した。 オーバーフローが不可能であることがわかっている場合は、 `decimal_check_overflow` 設定。 時チェックを無効とオーバーフローが起こり、結果は正しくあり:

``` sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

オーバーフローチェックは算術演算だけでなく、値の比較にも発生します:

``` sql
SELECT toDecimal32(1, 8) < 100
```

``` text
DB::Exception: Can't compare.
```

[元の記事](https://clickhouse.tech/docs/en/data_types/decimal/) <!--hide-->
