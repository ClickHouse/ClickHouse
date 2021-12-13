---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "IN\u6F14\u7B97\u5B50\u306E\u5B9F\u88C5"
---

# IN演算子を実装するための関数 {#functions-for-implementing-the-in-operator}

## で、ノチン、グロバリン、グロバルノチン {#in-functions}

セクションを参照 [演算子で](../operators/in.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

複数の列をグループ化できる関数。
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
タプルは、通常、in演算子の引数の中間値として、またはラムダ関数の仮パラメータのリストを作成するために使用されます。 タプルをテーブルに書き込むことはできません。

## tupleElement(タプル,n),演算子x.N {#tupleelementtuple-n-operator-x-n}

タプルから列を取得できる関数。
‘N’ 1から始まる列インデックスです。 Nは定数でなければなりません。 ‘N’ 定数でなければなりません。 ‘N’ 組のサイズより大きくない厳密なpostive整数でなければなりません。
関数を実行するコストはありません。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
