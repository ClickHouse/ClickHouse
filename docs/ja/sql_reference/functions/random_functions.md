---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 51
toc_title: "\u64EC\u4F3C\u4E71\u6570\u306E\u751F\u6210"
---

# 擬似乱数を生成するための関数 {#functions-for-generating-pseudo-random-numbers}

擬似乱数の非暗号生成器が使用される。

全ての機能を受け入れゼロの引数または一つの引数。
引数が渡された場合は、任意の型にすることができ、その値は何にも使用されません。
この引数の唯一の目的は、同じ関数の二つの異なるインスタンスが異なる乱数を持つ異なる列を返すように、共通の部分式の除去を防ぐことです。

## ランド {#rand}

すべてのuint32型の数値に均等に分布する、擬似ランダムなuint32数値を返します。
線形合同発生器を使用します。

## rand64 {#rand64}

すべてのuint64型の数値に均等に分布する、擬似ランダムなuint64数値を返します。
線形合同発生器を使用します。

## randconstantname {#randconstant}

値は、異なるブロックのためのものである、擬似ランダムuint32番号を返します。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
