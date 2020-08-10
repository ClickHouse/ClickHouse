---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 61
toc_title: arrayJoin
---

# arrayJoin関数 {#functions_arrayjoin}

これは非常に珍しい機能です。

通常の関数は行のセットを変更するのではなく、各行（map）の値を変更するだけです。
集計関数は、行のセットを圧縮します（折り畳みまたは縮小）。
その ‘arrayJoin’ 関数は、各行を受け取り、行のセット（展開）を生成します。

この関数は、引数として配列を受け取り、配列内の要素の数のために複数の行にソース行を伝播します。
列内のすべての値は、この関数が適用される列内の値を除いて単純にコピーされます。

クエリは複数を使用できます `arrayJoin` 機能。 この場合、変換は複数回実行されます。

メモselectクエリの配列結合の構文は、より広範な可能性を提供します。

例えば:

``` sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

``` text
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/array_join/) <!--hide-->
