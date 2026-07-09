---
description: '値を持つカラムからなる一時ストレージを作成します。'
keywords: ['values', 'table function']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

`Values` テーブル関数を使用すると、値を持つカラムからなる一時ストレージを作成できます。これは、簡単なテストやサンプルデータの生成に便利です。

:::note
Values は大文字と小文字を区別しない関数です。つまり、`VALUES` と `values` はどちらも有効です。
:::

<div id="syntax">
  ## 構文
</div>

`VALUES` テーブル関数の基本構文は次のとおりです。

```sql
VALUES([structure,] values...)
```

一般的な使用方法は次のとおりです。

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## 引数
</div>

* `column1_name Type1, ...` (任意) 。[String](/ja/sql-reference/data-types/string)
  カラム名と型を指定します。この引数を省略すると、カラム名は
  `c1`、`c2` などになります。
* `(value1_row1, value2_row1)`。[Tuples](/ja/sql-reference/data-types/tuple)
  任意の型の値を含むタプルです。

:::note
カンマ区切りのタプルは、単一の値に置き換えることもできます。この場合、
各値は新しい行として扱われます。詳細は [例](#examples) セクションを
参照してください。
:::

<div id="returned-value">
  ## 戻り値
</div>

* 指定された値を含む一時テーブルを返します。

<div id="examples">
  ## 例
</div>

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

`VALUES` は、タプルではなく単一の値を使って指定することもできます。たとえば次のとおりです。

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

または、行の指定 ([構文](#syntax) の `'column1_name Type1, column2_name Type2, ...'`) を行わない場合、カラム名は自動的に付けられます。

たとえば:

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

<div id="sql-standard-values-clause">
  ## SQL 標準 VALUES 句
</div>

ClickHouse ではバージョン 26.3 以降、PostgreSQL、MySQL、DuckDB、SQL Server と同様に、`FROM` 内でテーブル式として SQL 標準の `VALUES` 句もサポートしています。この構文は内部的に、前述の `values` テーブル関数を使用する形に書き換えられます。

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

CTEでも使用できます：

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

JOINでは:

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
`AS t(col1, col2, ...)` の後に続くカラムの別名は、派生テーブルのカラム名を指定する標準的な SQL 構文に従います。省略した場合、カラム名は `c1`、`c2` などになります。
:::

<div id="see-also">
  ## 関連項目
</div>

* [Values 形式](/ja/interfaces/formats/Values)