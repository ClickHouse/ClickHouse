---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Float32\u3001Float64"
---

# Float32、Float64 {#float32-float64}

[浮動小数点数](https://en.wikipedia.org/wiki/IEEE_754).

型はCの型と同等です:

-   `Float32` - `float`
-   `Float64` - `double`

可能な限り整数形式でデータを格納することをお勧めします。 たとえば、固定精度の数値を、金額やページの読み込み時間などの整数値にミリ秒単位で変換します。

## 浮動小数点数の使用 {#using-floating-point-numbers}

-   浮動小数点数を使用した計算では、丸め誤差が生じることがあります。

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

-   計算の結果は、計算方法（コンピュータシステムのプロセッサタイプおよびアーキテクチャ）に依存する。
-   浮動小数点計算では、無限大などの数値が生成されることがあります (`Inf`)と “not-a-number” (`NaN`). これは、計算結果を処理するときに考慮する必要があります。
-   テキストから浮動小数点数を解析する場合、結果が最も近いマシン表現可能な数値ではない可能性があります。

## NaNおよびInf {#data_type-float-nan-inf}

標準のSQLとは対照的に、ClickHouseは浮動小数点数の次のカテゴリをサポートしています:

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

    See the rules for `NaN` sorting in the section [ORDER BY clause](../sql_reference/statements/select/order-by.md).

[元の記事](https://clickhouse.tech/docs/en/data_types/float/) <!--hide-->
