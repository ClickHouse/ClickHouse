---
slug: /ja/sql-reference/functions/array-join
sidebar_position: 15
sidebar_label: arrayJoin
---

# arrayJoin 関数

これは非常に特殊な関数です。

通常の関数は行の集合を変えずに、各行の値だけを変更します（マップ）。
集計関数は行の集合を圧縮します（フォールドまたはリデュース）。
`arrayJoin` 関数は各行を取り、行の集合を生成します（アンフォールド）。

この関数は配列を引数に取り、配列の要素数に応じて元の行を複数の行に展開します。
この関数が適用されたカラム以外のすべてのカラムの値は単純にコピーされ、適用されたカラムの値は対応する配列の値に置き換えられます。

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

`arrayJoin` 関数は `WHERE` セクションを含むクエリのすべてのセクションに影響を与えます。サブクエリが1行を返したにもかかわらず、結果が2を示している点に注意してください。

例:

```sql
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Bobruisk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

``` text
┌─impressions─┐
│           2 │
└─────────────┘
```

クエリは複数の `arrayJoin` 関数を使用することができます。この場合、変換が複数回行われ、行が増えます。

例:

```sql
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

``` text
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Bobruisk │ Chrome  │
│           1 │ Bobruisk │ Firefox │
└─────────────┴──────────┴─────────┘
```
### 重要な注意点！
同じ式で複数の `arrayJoin` を使用すると、最適化のために期待される結果が得られないことがあります。その場合、結合結果に影響しない追加の操作で繰り返される配列式を修正することを検討してください - 例: `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

例:
```sql
SELECT
    arrayJoin(dice) as first_throw,
    /* arrayJoin(dice) as second_throw */ -- 技術的には正しいですが、結果セットを消してしまいます
    arrayJoin(arrayConcat(dice, [])) as second_throw -- 意図的に表現を変更して再評価を強制
FROM (
    SELECT [1, 2, 3, 4, 5, 6] as dice
);
```

SELECT クエリの [ARRAY JOIN](../statements/select/array-join.md) 構文に注意してください。これにより、より広い可能性が提供されます。
`ARRAY JOIN` は、同じ数の要素を持つ複数の配列を一度に変換することができます。

例:

```sql
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

``` text
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Bobruisk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

または [Tuple](../data-types/tuple.md) を使用することもできます。

例:

```sql
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

``` text
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Bobruisk │ Chrome  │
└─────────────┴──────────┴─────────┘
```
