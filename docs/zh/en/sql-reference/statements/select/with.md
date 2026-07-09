---
description: 'WITH 子句文档'
sidebar_label: 'WITH'
slug: /sql-reference/statements/select/with
title: 'WITH 子句'
doc_type: 'reference'
---

ClickHouse 支持公共表表达式 ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL))、公共标量表达式和递归查询。

<div id="common-table-expressions">
  ## 公共表表达式
</div>

公共表表达式表示具名子查询。
在 `SELECT` 查询中，凡是允许使用表表达式的地方，都可以通过名称引用它们。
具名子查询可以在当前查询的作用域内，或在子查询的作用域内通过名称引用。

在 `SELECT` 查询中，每次引用公共表表达式时，如果该 CTE 未显式定义为 materialized (参见[物化公共表表达式](#materialized-common-table-expressions)) ，都会被替换为其定义中的子查询。
通过在标识符解析过程中隐藏当前 CTE，可以防止递归。

请注意，CTE 并不能保证在所有引用位置都产生相同结果，因为每次使用时都会重新执行一次查询。

<div id="common-table-expressions-syntax">
  ### 语法
</div>

```sql
WITH <identifier> AS [MATERIALIZED] <subquery expression>
```

<div id="common-table-expressions-example">
  ### 示例
</div>

下面是一个子查询会被重新执行的示例：

```sql
WITH cte_numbers AS
(
    SELECT
        num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT
    count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers)
```

如果 CTE 传递的是实际结果，而不只是代码片段，你将始终看到 `1000000`

然而，由于我们引用了 `cte_numbers` 两次，每次都会重新生成随机数，因此会看到不同的随机结果，例如 `280501, 392454, 261636, 196227` 等等……

<div id="materialized-common-table-expressions">
  ## 物化公共表表达式
</div>

默认情况下，ClickHouse 会在每次引用 CTE 时内联其子查询，因此每次都会重新执行。
添加 `MATERIALIZED` 关键字后，ClickHouse 会将 CTE 子查询**仅执行一次**，把结果存储在临时表中，并让所有引用都从该表读取结果。
当同一个 CTE 在一个查询中被多次引用时 (例如在自连接或多个 `IN` 子查询中) ，这尤其有用，因为底层计算只会执行一次。

:::note
物化 CTE 是一项 **Experimental** 功能。
它要求启用 [analyzer](/zh/operations/analyzer) 以及设置 `enable_materialized_cte`。
:::

<div id="common-table-expressions-syntax">
  ### 语法
</div>

```sql
WITH <identifier> AS MATERIALIZED (<subquery>)
SELECT ...
```

<div id="materialized-cte-when-to-use">
  ### 何时使用
</div>

materialized CTE 在以下情况下最有用：

* 在一个查询中，同一个 CTE 被**引用多次**。
  如果没有 `MATERIALIZED`，每次引用都会各自重新执行该子查询。
* CTE 包含像 `generateRandom` 这样的**非确定性**函数。
  materialized 后可确保所有引用看到的数据一致。
* CTE 涉及**高成本计算** (聚合、JOIN、大范围扫描) ，不应重复执行。

:::tip
如果 materialized CTE 只被引用一次，ClickHouse 会自动将其内联回普通子查询，以避免不必要的开销。
:::

<div id="common-table-expressions-example">
  ### 示例
</div>

**示例 1：** 对 物化 CTE 进行自连接

如果不使用 `MATERIALIZED`，连接两侧都会分别执行该子查询。
使用 `MATERIALIZED` 时，表只会扫描一次，连接两侧都会从同一个临时表中读取。

```sql
SET enable_materialized_cte = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE = Memory;
INSERT INTO users VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

WITH
    a AS MATERIALIZED (SELECT * FROM users WHERE name = 'Alice')
SELECT count() FROM a AS l JOIN a AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       1 │
└─────────┘
```

**示例 2：** 使用非确定性函数获得确定性结果

常规 CTE 在使用 `generateRandom` 时，每次引用都会产生不同的结果。
将 CTE 物化可确保结果保持一致：

```sql
SET enable_materialized_cte = 1;

WITH cte_numbers AS MATERIALIZED
(
    SELECT num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers);
```

由于这两个引用都从同一份 materialized 数据中读取，因此结果始终是 `1000000`。

**示例 3：** 串联 物化 CTE

物化 CTE 可以引用其他 物化 CTE。
ClickHouse 会解析依赖项，并按正确的顺序将其物化：

```sql
SET enable_materialized_cte = 1;

WITH
    a AS MATERIALIZED (SELECT uid, name FROM users),
    b AS MATERIALIZED (SELECT uid FROM a)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

CTE 的定义顺序无关紧要——允许向前引用：

```sql
SET enable_materialized_cte = 1;

WITH
    b AS MATERIALIZED (SELECT uid FROM a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

<div id="materialized-cte-restrictions">
  ### 限制
</div>

* **需要启用 Experimental 设置**：必须启用设置 `enable_materialized_cte`。
* **需要启用 analyzer**：物化 CTE 仅在启用 [analyzer](/zh/operations/analyzer) (`enable_analyzer = 1`) 时才能使用。
* **不支持与 `RECURSIVE` 结合使用**：不允许同时使用 `MATERIALIZED` 和 `RECURSIVE` 关键字，否则会引发 `UNSUPPORTED_METHOD` 异常。
* **禁止使用关联 CTE**：物化 CTE 不能引用外层查询作用域中的列。

<div id="common-scalar-expressions">
  ## 公共标量表达式
</div>

ClickHouse 允许你在 `WITH` 子句中为任意标量表达式声明别名。
公共标量表达式可以在查询中的任何位置引用。

:::note
如果公共标量表达式引用的不是常量字面量，该表达式可能会导致出现[自由变量](https://en.wikipedia.org/wiki/Free_variables_and_bound_variables)。
ClickHouse 会在尽可能靠近的作用域中解析任何标识符，这意味着一旦发生名称冲突，自由变量可能会引用到意外的实体，或者导致关联子查询。
建议将 CSE 定义为 [lambda 函数](/zh/sql-reference/functions/overview#arrow-operator-and-lambda) (仅在启用 [analyzer](/zh/operations/analyzer) 时可行) ，并将所有使用到的标识符都绑定进去，以使表达式中的标识符解析行为更可预测。
:::

<div id="common-table-expressions-syntax">
  ### 语法
</div>

```sql
WITH <expression> AS <identifier>
```

<div id="common-table-expressions-example">
  ### 示例
</div>

**示例 1：** 将常量表达式用作“变量”

```sql
WITH '2019-08-01 15:23:00' AS ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**示例 2：** 使用高阶函数来限制标识符

```sql
WITH
    '.txt' as extension,
    (id, extension) -> concat(lower(id), extension) AS gen_name
SELECT gen_name('test', '.sql') as file_name;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**示例 3：** 将高阶函数与自由变量结合使用

下面的示例查询表明，未绑定的标识符会解析为最近作用域中的实体。
这里，`extension` 在 `gen_name` lambda 函数体中未绑定。
尽管在 `generated_names` 的定义和使用所在的作用域内，`extension` 被定义为公共标量表达式 `'.txt'`，但它会被解析为表 `extension_list` 的一列，因为它在 `generated_names` 子查询中可用。

```sql
CREATE TABLE extension_list
(
    extension String
)
ORDER BY extension
AS SELECT '.sql';

WITH
    '.txt' as extension,
    generated_names as (
        WITH
            (id) -> concat(lower(id), extension) AS gen_name
        SELECT gen_name('test') as file_name FROM extension_list
    )
SELECT file_name FROM generated_names;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**示例 4：**将 sum(bytes) 表达式结果从 SELECT 子句的列列表中剔除

```sql
WITH sum(bytes) AS s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**示例 5：**使用标量子查询的结果

```sql
/* this example would return TOP 10 of most huge tables */
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

**示例 6：** 在子查询里复用表达式

```sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

<div id="recursive-queries">
  ## 递归查询
</div>

可选的 `RECURSIVE` 修饰符允许 WITH 查询引用自身的输出结果。示例：

**示例：** 对 1 到 100 的整数求和

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table WHERE number < 100
)
SELECT sum(number) FROM test_table;
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

:::note
递归 CTE 依赖版本 **`24.3`** 中引入的[查询分析器](/zh/operations/analyzer)。如果你使用的是 **`24.3+`** 版本，并遇到 **`(UNKNOWN_TABLE)`** 或 **`(UNSUPPORTED_METHOD)`** 异常，则说明你的 instance、Role 或 profile 上禁用了 analyzer。要启用 analyzer，请开启 **`allow_experimental_analyzer`** 设置，或将 **`compatibility`** 设置更新到较新的版本。
从 `24.8` 版本开始，analyzer 已正式提升为 production 功能，且设置 `allow_experimental_analyzer` 已重命名为 `enable_analyzer`。
:::

递归 `WITH` 查询的一般形式总是先是一个非递归项，然后是 `UNION ALL``，`最后是一个递归项，其中只有递归项可以引用查询自身的输出。递归 CTE 查询的执行方式如下：

1. 计算非递归项，并将非递归项查询的结果放入临时工作表中。
2. 只要工作表不为空，就重复以下步骤：
   1. 计算递归项，并将工作表的当前内容替换为递归自引用。将递归项查询的结果放入临时中间表中。
   2. 用中间表的内容替换工作表的内容，然后清空中间表。

递归查询通常用于处理层级数据或树状结构数据。例如，我们可以编写一个执行树遍历的查询：

**示例：** 树遍历

首先，创建树表：

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

我们可以使用如下查询遍历这些树：

**示例：** 树遍历

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

<div id="search-order">
  ### 遍历顺序
</div>

为了按深度优先顺序排列，我们需要为结果中的每一行计算一个包含已访问行的数组：

**示例：** 树遍历的深度优先顺序

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

要按广度优先顺序排列，标准做法是添加一个用于跟踪搜索深度的列：

**示例：** 树遍历的广度优先顺序

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

<div id="cycle-detection">
  ### 循环检测
</div>

首先，创建 graph 表：

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

我们可以使用如下查询遍历该图：

**示例：** 未进行环检测的图遍历

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

但如果我们在该图中加入一个环路，前面的查询就会因 `Maximum recursive CTE evaluation depth` 错误而失败：

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
Code: 306. DB::Exception: Received from localhost:9000. DB::Exception: Maximum recursive CTE evaluation depth (1000) exceeded, during evaluation of search_graph AS (SELECT from, to, label FROM graph AS g UNION ALL SELECT g.from, g.to, g.label FROM graph AS g, search_graph AS sg WHERE g.from = sg.to). Consider raising max_recursive_cte_evaluation_depth setting.: While executing RecursiveCTESource. (TOO_DEEP_RECURSION)
```

处理循环的标准方法是构造一个包含已访问节点的数组：

**示例：** 带循环检测的图遍历

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

<div id="infinite-queries">
  ### 无限查询
</div>

如果在外层查询中使用 `LIMIT`，则也可以使用无限递归 CTE 查询：

**示例：** 无限递归 CTE 查询

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

<div id="trailing-comma">
  ## 尾随逗号
</div>

`WITH` 子句中最后一个元素后允许加逗号：

```sql
WITH
    (SELECT sum(number) FROM numbers(10)) AS total,
    total * 2 AS doubled,
SELECT total, doubled;
```