---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u30A2\u30EC\u30A4\u30B8\u30E7\u30A4\u30F3"
---

# arrayJoin関数 {#functions_arrayjoin}

これは非常に珍しい機能です。

通常の関数は行のセットを変更するのではなく、各行（map）の値を変更するだけです。
集計関数は、行のセットを圧縮します（foldまたはreduce）。
その ‘arrayJoin’ 関数は、各行を取り、行のセットを生成します（展開）。

この関数は、配列を引数として受け取り、配列内の要素数に対してソース行を複数の行に伝播します。
この関数が適用される列の値を除いて、列内のすべての値は単純にコピーされます。

クエリでは、複数の `arrayJoin` 機能。 この場合、変換は複数回実行されます。

SELECTクエリの配列結合構文に注意してください。

例:

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
