---
description: 'ClickHouse の浮動小数点データ型（Float32、Float64、BFloat16）に関するドキュメント'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Float32 | Float64 | BFloat16 型'
doc_type: 'reference'
---

:::note
正確な計算が必要な場合、特に高い精度が求められる金融データや業務データを扱う場合は、[Decimal](../data-types/decimal.md) の使用を検討してください。

以下に示すように、[浮動小数点数](https://en.wikipedia.org/wiki/IEEE_754) は不正確な結果をもたらす可能性があります。

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
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

ClickHouse と C における対応する型は以下のとおりです。

* `Float32` — `float`.
* `Float64` — `double`.

ClickHouse の Float 型には、以下の別名があります。

* `Float32` — `FLOAT`, `REAL`, `SINGLE`.
* `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

テーブルの作成時には、浮動小数点数の数値パラメータを設定できます (たとえば `FLOAT(12)`、`FLOAT(15, 22)`、`DOUBLE(12)`、`DOUBLE(4, 18)`) が、ClickHouse はこれらを無視します。

<div id="using-floating-point-numbers">
  ## 浮動小数点数の使用
</div>

* 浮動小数点数を使った計算では、丸め誤差が生じることがあります。

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* 計算結果は、計算方法 (コンピューターシステムのプロセッサの種類やアーキテクチャ) によって異なります。
* 浮動小数点計算では、無限大 (`Inf`) や「非数」 (`NaN`) のような値になることがあります。計算結果を処理する際は、この点を考慮する必要があります。
* テキストから浮動小数点数をパースする際、機械上で表現可能な最も近い値にならないことがあります。

<div id="nan-and-inf">
  ## NaN と Inf
</div>

標準的な SQL とは異なり、ClickHouse は次のカテゴリの浮動小数点数をサポートしています。

* `Inf` – 無限大。

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — 負の無限大。

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — 非数。

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

`NaN` のソートに関するルールについては、[ORDER BY 句](../../sql-reference/statements/select/order-by.md) を参照してください。

<div id="nan-values-in-set-semantics">
  ## 集合セマンティクスにおける NaN 値
</div>

IEEE 754 標準では、スカラー比較 `NaN = NaN` が `false` を返すように `NaN` が定義されています。
ClickHouse も、`=` 演算子についてはこの規則に従います。

ただし、`NaN` は単一の値ではなく、指数がすべて 1 で、かつ
仮数が 0 以外である任意のビットパターンを指します。異なる操作や異なる CPU アーキテクチャでは、
符号ビットや仮数ペイロードが異なる `NaN`
値が生成されることがあります。たとえば、次のようなものです。

* `0./0.` は、ほとんどの x86 プラットフォームで符号ビットが 1 の `NaN` を生成します。
* リテラル `nan` は、符号ビットが 0 の `NaN` を生成します。
* [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230) 以降、AArch64 NEON パスの
  `log` は、負の入力に対して glibc のスカラー `log` とは符号ビットの異なる `NaN` を返します。

ClickHouse のハッシュテーブルはキーをバイト単位で比較するため、異なる `NaN` のビットパターンは
異なるバケットにハッシュされ、`DISTINCT`、`GROUP BY`、`uniqExact`、`countDistinct`、および `Float` キーに対する equi-`JOIN` などの
集合セマンティクスを持つ操作では、別個の値として扱われます。

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

これは IEEE 754 と整合しています (すべての `NaN` は、自身を含む他のどの値とも等しくありません) 
が、意外に思えるかもしれません。集合セマンティクスの操作ですべての `NaN` 値を等しいものとして扱う必要がある場合は、
クエリ内でそれらを正規化してください。

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

同じアプローチは、`DISTINCT`、`GROUP BY`、`JOIN` のキーにも適用できます。

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16` は、8 ビットの指数、符号ビット、7 ビットの仮数を持つ 16 ビット浮動小数点データ型です。
機械学習や AI アプリケーションに適しています。

ClickHouse は `Float32` と `BFloat16` 間の変換をサポートしており、[`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) または [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16) 関数を使用して行えます。

:::note
そのほかのほとんどの演算はサポートされていません。
:::