---
description: 'arrayJoin関数のドキュメント'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'arrayJoin関数'
doc_type: 'reference'
---

これは非常に特殊な関数です。

通常の関数は行の集合を変化させず、各行の値を変更するだけです (map) 。
集約関数は行の集合を圧縮します (fold または reduce) 。
`arrayJoin` 関数は各行を受け取り、行の集合を生成します (unfold) 。

この関数は引数として配列を受け取り、配列の要素数に応じて元の行を複数の行へ展開します。
すべてのカラムの値はそのままコピーされますが、この関数が適用されるカラムの値だけは、対応する配列要素の値に置き換えられます。

:::note
配列が空の場合、`arrayJoin` は行を生成しません。
配列型のデフォルト値を含む 1 行を返すには、たとえば `arrayJoin(emptyArrayToSingle(...))` のように、[emptyArrayToSingle](./array-functions.md#emptyArrayToSingle) でラップします。
:::

例:

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

`arrayJoin` 関数は、`WHERE` 句を含むクエリのすべての部分に影響します。以下のクエリでは、サブクエリが 1 行しか返していないにもかかわらず、結果が `2` になることに注目してください。

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

1つのクエリで複数の `arrayJoin` 関数を使用できます。この場合、変換は複数回実行され、行数が増えます。
例えば:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### ベストプラクティス
</div>

同じ式に対して複数の `arrayJoin` を使用すると、共通部分式が削除されるため、期待どおりの結果にならないことがあります。
そのような場合は、JOIN結果に影響しない追加の操作を使って、繰り返し使う配列式を変更することを検討してください。たとえば、 `arrayJoin(arraySort(arr))`、`arrayJoin(arrayConcat(arr, []))`

例:

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

SELECTクエリ内の[`ARRAY JOIN`](../statements/select/array-join.md)構文に注目してください。これにより、より柔軟なことが可能になります。
`ARRAY JOIN`を使用すると、同じ要素数を持つ複数の配列をまとめて変換できます。

例:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

または [`Tuple`](../data-types/tuple.md) を使用できます

例:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

ClickHouse における `arrayJoin` という名前は、概念的には JOIN演算 に似ていますが、それを単一行内の配列に適用したものであることに由来します。従来の JOIN では異なるテーブルの行を結合しますが、`arrayJoin` は 1 行内の配列の各要素を&quot;結合&quot;し、他のカラムの値を複製しながら、配列要素ごとに 1 行ずつ複数の行を生成します。ClickHouse では [&#96;ARRAY JOIN&#96;](/ja/sql-reference/statements/select/array-join) 句の構文も提供されており、一般的な SQL の JOIN 用語を使うことで、従来の JOIN演算 との関係をさらに明示的にしています。この処理は配列を&quot;展開する&quot;とも呼ばれますが、関数名と句の両方で &quot;join&quot; という語が使われているのは、テーブルを配列要素と結合するのに似た形となり、その結果、JOIN演算 と同様にデータセットが実質的に拡張されるためです。