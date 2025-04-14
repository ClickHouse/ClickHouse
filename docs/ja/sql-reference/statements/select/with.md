---
slug: /ja/sql-reference/statements/select/with
sidebar_label: WITH
---

# WITH句

ClickHouseは共通テーブル式（CTE）をサポートしており、`WITH`句で定義されたコードを`SELECT`クエリの他のすべての使用箇所で置き換えます。名前付きサブクエリは、テーブルオブジェクトが許可される場所に、現在のクエリコンテキストおよび子クエリコンテキストに含めることができます。現在のレベルのCTEをWITH式から隠すことで再帰を防止します。

CTEはすべての呼び出し箇所で同じ結果を保証しないことに注意してください。使用ケースごとにクエリが再実行されるためです。

以下に、そのような動作の例を示します。
``` sql
with cte_numbers as
(
    select
        num
    from generateRandom('num UInt64', NULL)
    limit 1000000
)
select
    count()
from cte_numbers
where num in (select num from cte_numbers)
```
CTEが単にコード片ではなく正確に結果を返すなら、常に`1000000`を見ることになるでしょう。

しかし、`cte_numbers`を2回参照しているため、毎回ランダムな数が生成され、それに応じて異なるランダムな結果が表示されます。例えば、`280501, 392454, 261636, 196227`などです。

## 文法

``` sql
WITH <expression> AS <identifier>
```
または
``` sql
WITH <identifier> AS <subquery expression>
```

## 例

**例1:** 定数式を「変数」として使用する

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**例2:** `SELECT`句のカラムリストからsum(bytes)の式結果を排除する

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**例3:** スカラサブクエリの結果を使用する

``` sql
/* この例では最も大きなテーブルのTOP 10を返します */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10;
```

**例4:** サブクエリ内で式を再利用する

``` sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

## 再帰クエリ

オプションのRECURSIVE修飾子により、WITHクエリが自身の出力を参照することができます。例：

**例:** 1から100までの整数の合計

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table WHERE number < 100
)
SELECT sum(number) FROM test_table;
```

``` text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

再帰的な`WITH`クエリの一般的な形式は、常に非再帰の項、次に`UNION ALL`、次に再帰の項が続き、再帰の項のみがクエリの自身の出力を参照できます。再帰CTEクエリは次のように実行されます：

1. 非再帰の項を評価します。非再帰の項クエリの結果を一時的な作業テーブルに配置します。
2. 作業テーブルが空でない限り、これらのステップを繰り返します：
    1. 再帰の項を評価し、作業テーブルの現在の内容を再帰的な自己参照として使用します。再帰の項クエリの結果を一時的な中間テーブルに配置します。
    2. 作業テーブルの内容を中間テーブルの内容で置き換え、中間テーブルを空にします。

再帰クエリは通常、階層データや木構造のデータを扱うために使用されます。例えば、木構造を探索するクエリを書くことができます：

**例:** 木構造探索

まず、ツリーテーブルを作成しましょう：

```sql
DROP TABLE IF EXISTS tree;
CREATE TABLE tree
(
    id UInt64,
    parent_id Nullable(UInt64),
    data String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');
```

このツリーを探索するためのクエリは以下の通りです：

**例:** 木構造探索
```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree;
```

```text
┌─id─┬─parent_id─┬─data──────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │
│  1 │         0 │ Child_1   │
│  2 │         0 │ Child_2   │
│  3 │         1 │ Child_1_1 │
└────┴───────────┴───────────┘
```

### 探索順序

深さ優先順を作成するためには、訪問済みの行の配列を各結果行に計算します：

**例:** 深さ優先順による木構造探索
```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id])
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY path;
```

```text
┌─id─┬─parent_id─┬─data──────┬─path────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │ [0]     │
│  1 │         0 │ Child_1   │ [0,1]   │
│  3 │         1 │ Child_1_1 │ [0,1,3] │
│  2 │         0 │ Child_2   │ [0,2]   │
└────┴───────────┴───────────┴─────────┘
```

幅優先順を作成するためには、探索の深さを追跡するカラムを追加するのが標準的なアプローチです：

**例:** 幅優先順による木構造探索
```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path, toUInt64(0) AS depth
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id]), depth + 1
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY depth;
```

```text
┌─id─┬─link─┬─data──────┬─path────┬─depth─┐
│  0 │ ᴺᵁᴸᴸ │ ROOT      │ [0]     │     0 │
│  1 │    0 │ Child_1   │ [0,1]   │     1 │
│  2 │    0 │ Child_2   │ [0,2]   │     1 │
│  3 │    1 │ Child_1_1 │ [0,1,3] │     2 │
└────┴──────┴───────────┴─────────┴───────┘
```

### サイクル検出

まずグラフテーブルを作成しましょう：

```sql
DROP TABLE IF EXISTS graph;
CREATE TABLE graph
(
    from UInt64,
    to UInt64,
    label String
) ENGINE = MergeTree ORDER BY (from, to);

INSERT INTO graph VALUES (1, 2, '1 -> 2'), (1, 3, '1 -> 3'), (2, 3, '2 -> 3'), (1, 4, '1 -> 4'), (4, 5, '4 -> 5');
```

このグラフを探索するためのクエリは以下の通りです：

**例:** サイクル検出なしのグラフ探索
```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
    UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```
```text
┌─from─┬─to─┬─label──┐
│    1 │  4 │ 1 -> 4 │
│    1 │  2 │ 1 -> 2 │
│    1 │  3 │ 1 -> 3 │
│    2 │  3 │ 2 -> 3 │
│    4 │  5 │ 4 -> 5 │
└──────┴────┴────────┘
```

しかし、グラフにサイクルを加えると、前のクエリは`最大再帰CTE評価深度`エラーで失敗します：

```sql
INSERT INTO graph VALUES (5, 1, '5 -> 1');

WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
コード: 306. DB::Exception: 受信しました: localhost:9000から. DB::Exception: 最大再帰CTE評価深度 (1000) を超えました、search_graph AS (SELECT from, to, label FROM graph AS g UNION ALL SELECT g.from, g.to, g.label FROM graph AS g, search_graph AS sg WHERE g.from = sg.to) の評価中。max_recursive_cte_evaluation_depth設定を上げることを検討してください。: RecursiveCTESourceを実行中に (TOO_DEEP_RECURSION)
```

循環を処理する標準的な方法は、既に訪問したノードの配列を計算することです：

**例:** サイクル検出付きグラフ探索
```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label, false AS is_cycle, [tuple(g.from, g.to)] AS path FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label, has(path, tuple(g.from, g.to)), arrayConcat(sg.path, [tuple(g.from, g.to)])
    FROM graph g, search_graph sg
    WHERE g.from = sg.to AND NOT is_cycle
)
SELECT * FROM search_graph WHERE is_cycle ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┬─is_cycle─┬─path──────────────────────┐
│    1 │  4 │ 1 -> 4 │ true     │ [(1,4),(4,5),(5,1),(1,4)] │
│    4 │  5 │ 4 -> 5 │ true     │ [(4,5),(5,1),(1,4),(4,5)] │
│    5 │  1 │ 5 -> 1 │ true     │ [(5,1),(1,4),(4,5),(5,1)] │
└──────┴────┴────────┴──────────┴───────────────────────────┘
```

### 無限クエリ

外部クエリで`LIMIT`を使用する場合、無限再帰CTEクエリを使用することも可能です：

**例:** 無限再帰CTEクエリ
```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table
)
SELECT sum(number) FROM (SELECT number FROM test_table LIMIT 100);
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```
