---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 41
toc_title: Float32,Float64
---

# Float32,Float64 {#float32-float64}

[浮動小数点数](https://en.wikipedia.org/wiki/IEEE_754).

型はcの型と同じです:

-   `Float32` - `float`
-   `Float64` - `double`

可能な限り、データを整数形式で格納することをお勧めします。 たとえば、固定精度の数値を、金額やページの読み込み時間などの整数値にミリ秒単位で変換します。

## 浮動小数点数の使用 {#using-floating-point-numbers}

-   浮動小数点数を使用した計算では、丸め誤差が発生することがあります。

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

-   計算の結果は、計算方法（プロセッサの種類とコンピュータシステムのアーキテクチャ）に依存します。
-   浮動小数点の計算は、無限大などの数値になる可能性があります (`Inf`） “not-a-number” (`NaN`). これは、計算の結果を処理する際に考慮する必要があります。
-   テキストから浮動小数点数を解析する場合、結果は最も近いマシン表現可能な数値ではない可能性があります。

## NaNおよびInf {#data_type-float-nan-inf}

標準sqlとは対照的に、clickhouseは浮動小数点数の次のカテゴリをサポートしています:

-   `Inf` – Infinity.

<!-- -->

``` sql
SELECT 0.5 / 0
```

``` text
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

-   `-Inf` – Negative infinity.

<!-- -->

``` sql
SELECT -0.5 / 0
```

``` text
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

-   `NaN` – Not a number.

<!-- -->

``` sql
SELECT 0 / 0
```

``` text
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

    See the rules for `NaN` sorting in the section [ORDER BY clause](../sql_reference/statements/select.md).

[元の記事](https://clickhouse.tech/docs/en/data_types/float/) <!--hide-->
