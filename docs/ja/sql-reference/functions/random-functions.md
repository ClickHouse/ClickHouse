---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "\u64EC\u4F3C\u4E71\u6570\u306E\u751F\u6210"
---

# 擬似乱数を生成するための関数 {#functions-for-generating-pseudo-random-numbers}

擬似乱数の非暗号生成器が使用される。

全ての機能を受け入れゼロの引数または一つの引数。
引数が渡された場合は、任意の型にすることができ、その値は何にも使用されません。
この引数の唯一の目的は、同じ関数の二つの異なるインスタンスが異なる乱数を持つ異なる列を返すように、共通の部分式の除去を防ぐことです。

## ランド {#rand}

すべてのUInt32型の数値に均等に分布する擬似ランダムなUInt32数値を返します。
線形合同ジェネレータを使用します。

## rand64 {#rand64}

すべてのUInt64型の数値に均等に分布する擬似ランダムなUInt64数値を返します。
線形合同ジェネレータを使用します。

## ランドコンスタント {#randconstant}

ランダムな値を持つ定数列を生成します。

**構文**

``` sql
randConstant([x])
```

**パラメータ**

-   `x` — [式](../syntax.md#syntax-expressions) の何れかに終って [対応するデータ型](../data-types/index.md#data_types). 結果の値は破棄されますが、式自体がバイパスに使用されている場合は破棄されます [共通の部分式の除去](index.md#common-subexpression-elimination) 関数が一つのクエリで複数回呼び出された場合。 任意パラメータ。

**戻り値**

-   擬似乱数。

タイプ: [UInt32](../data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT rand(), rand(1), rand(number), randConstant(), randConstant(1), randConstant(number)
FROM numbers(3)
```

結果:

``` text
┌─────rand()─┬────rand(1)─┬─rand(number)─┬─randConstant()─┬─randConstant(1)─┬─randConstant(number)─┐
│ 3047369878 │ 4132449925 │   4044508545 │     2740811946 │      4229401477 │           1924032898 │
│ 2938880146 │ 1267722397 │   4154983056 │     2740811946 │      4229401477 │           1924032898 │
│  956619638 │ 4238287282 │   1104342490 │     2740811946 │      4229401477 │           1924032898 │
└────────────┴────────────┴──────────────┴────────────────┴─────────────────┴──────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
