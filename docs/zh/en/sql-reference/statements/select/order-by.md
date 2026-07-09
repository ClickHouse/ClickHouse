---
description: 'ORDER BY 子句文档'
sidebar_label: 'ORDER BY'
slug: /sql-reference/statements/select/order-by
title: 'ORDER BY 子句'
doc_type: 'reference'
---

`ORDER BY` 子句包含：

* 表达式列表，例如 `ORDER BY visits, search_phrase`；
* 引用 `SELECT` 子句中列的数字列表，例如 `ORDER BY 2, 1`；或者
* `ALL`，表示 `SELECT` 子句中的所有列，例如 `ORDER BY ALL`。

要禁用按列号排序，请将设置项 [enable&#95;positional&#95;arguments](/zh/operations/settings/settings#enable_positional_arguments) 设为 0。
要禁用按 `ALL` 排序，请将设置项 [enable&#95;order&#95;by&#95;all](/zh/operations/settings/settings#enable_order_by_all) 设为 0。

`ORDER BY` 子句可以使用 `DESC` (降序) 或 `ASC` (升序) 修饰符来指定排序方向。
除非显式指定排序顺序，否则默认使用 `ASC`。
排序方向仅适用于单个表达式，而不适用于整个列表，例如 `ORDER BY Visits DESC, SearchPhrase`。
此外，排序区分大小写。

对于排序表达式值相同的行，将以任意的非确定性顺序返回。
如果在 `SELECT` 语句中省略 `ORDER BY` 子句，行顺序同样是任意的非确定性顺序。

<div id="sorting-of-special-values">
  ## 特殊值的排序
</div>

对于 `非数字` 和 `NULL` 的排序顺序，有两种方式：

* 默认情况下，或使用 `NULLS LAST` 修饰符时：先是普通值，再是 `非数字`，最后是 `NULL`。
* 使用 `NULLS FIRST` 修饰符时：先是 `NULL`，再是 `非数字`，最后是其他值。

<div id="example">
  ### 示例
</div>

对于这张表

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

运行查询 `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST`，即可得到：

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

对浮点数进行排序时，非数字会与其他值分开。无论采用何种排序顺序，非数字都会排在最后。换句话说，在升序排序中，它们会被视为大于所有其他数字；而在降序排序中，它们会被视为小于其余数字。

<div id="collation-support">
  ## 排序规则 支持
</div>

对于按 [String](../../../sql-reference/data-types/string.md) 值排序，可以指定 collation (比较规则) 。示例：`ORDER BY SearchPhrase COLLATE 'tr'` — 表示在假定字符串采用 UTF-8 编码的前提下，使用土耳其字母表按关键字升序排序，且不区分大小写。在 ORDER BY 中，每个表达式都可以独立指定或不指定 `COLLATE`。如果指定了 `ASC` 或 `DESC`，则 `COLLATE` 写在其后。使用 `COLLATE` 时，排序始终不区分大小写。

[LowCardinality](../../../sql-reference/data-types/lowcardinality.md)、[Nullable](../../../sql-reference/data-types/nullable.md)、[Array](../../../sql-reference/data-types/array.md) 和 [Tuple](../../../sql-reference/data-types/tuple.md) 支持 Collate。

我们只建议将 `COLLATE` 用于对少量行做最终排序，因为使用 `COLLATE` 的排序效率低于普通的按字节排序。

<div id="collation-examples">
  ## 排序规则示例
</div>

仅使用 [String](../../../sql-reference/data-types/string.md) 值的示例：

输入表：

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

[Nullable](../../../sql-reference/data-types/nullable.md) 示例：

输入表：

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │ ABC  │
│ 4 │ 123a │
│ 5 │ abc  │
│ 6 │ ᴺᵁᴸᴸ │
│ 7 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 4 │ 123a │
│ 5 │ abc  │
│ 3 │ ABC  │
│ 1 │ bca  │
│ 7 │ BCA  │
│ 6 │ ᴺᵁᴸᴸ │
│ 2 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

[Array](../../../sql-reference/data-types/array.md) 示例：

输入表：

```text
┌─x─┬─s─────────────┐
│ 1 │ ['Z']         │
│ 2 │ ['z']         │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 7 │ ['']          │
└───┴───────────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s─────────────┐
│ 7 │ ['']          │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 2 │ ['z']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 1 │ ['Z']         │
└───┴───────────────┘
```

[LowCardinality](../../../sql-reference/data-types/lowcardinality.md) 字符串示例：

输入表：

```response
┌─x─┬─s───┐
│ 1 │ Z   │
│ 2 │ z   │
│ 3 │ a   │
│ 4 │ A   │
│ 5 │ za  │
│ 6 │ zaa │
│ 7 │     │
└───┴─────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───┐
│ 7 │     │
│ 3 │ a   │
│ 4 │ A   │
│ 2 │ z   │
│ 1 │ Z   │
│ 5 │ za  │
│ 6 │ zaa │
└───┴─────┘
```

使用 [Tuple](../../../sql-reference/data-types/tuple.md) 的示例：

```response title="Response"
┌─x─┬─s───────┐
│ 1 │ (1,'Z') │
│ 2 │ (1,'z') │
│ 3 │ (1,'a') │
│ 4 │ (2,'z') │
│ 5 │ (1,'A') │
│ 6 │ (2,'Z') │
│ 7 │ (2,'A') │
└───┴─────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───────┐
│ 3 │ (1,'a') │
│ 5 │ (1,'A') │
│ 2 │ (1,'z') │
│ 1 │ (1,'Z') │
│ 7 │ (2,'A') │
│ 4 │ (2,'z') │
│ 6 │ (2,'Z') │
└───┴─────────┘
```

<div id="implementation-details">
  ## 实现细节
</div>

如果在 `ORDER BY` 之外还指定了足够小的 [LIMIT](../../../sql-reference/statements/select/limit.md)，则占用的 RAM 会更少。否则，消耗的内存量与参与排序的数据量成正比。对于分布式查询处理，如果省略了 [GROUP BY](/zh/sql-reference/statements/select/group-by)，排序会在远程服务器上部分完成，结果则会在请求发起服务器上合并。这意味着，在分布式排序中，需要排序的数据量可能会大于单台服务器的内存容量。

如果 RAM 不足，可以在外部内存中执行排序 (在 disk 上创建临时 File) 。为此，请使用设置 `max_bytes_before_external_sort`。如果将其设置为 0 (默认值) ，则会禁用外部排序。如果启用了该功能，当待排序的数据量达到指定字节数时，已收集的数据会被排序并转储到临时 File 中。读取完所有数据后，所有已排序的文件会被合并，然后输出结果。文件会写入 config 中的 `/var/lib/clickhouse/tmp/` directory (默认如此，但你可以使用 `tmp_path` parameter 更改此设置) 。你还可以仅在查询超过 memory limit 时使用落盘，例如，`max_bytes_ratio_before_external_sort=0.6` 表示只有当查询达到 `60%` memory limit (user/sever) 后才会启用落盘。

运行查询时，实际使用的内存可能会超过 `max_bytes_before_external_sort`。因此，该设置的值必须明显小于 `max_memory_usage`。例如，如果你的服务器有 128 GB RAM，并且只需要运行单个查询，可以将 `max_memory_usage` 设置为 100 GB，将 `max_bytes_before_external_sort` 设置为 80 GB。

外部排序的效率远低于在 RAM 中排序。

<div id="optimization-of-data-reading">
  ## 数据读取优化
</div>

如果 `ORDER BY` 表达式具有与表排序键一致的前缀，则可以通过使用 [optimize&#95;read&#95;in&#95;order](../../../operations/settings/settings.md#optimize_read_in_order) 设置来优化查询。

启用 `optimize_read_in_order` 设置后，ClickHouse server 会使用表索引，并按 `ORDER BY` 键的顺序读取数据。这样在指定 [LIMIT](../../../sql-reference/statements/select/limit.md) 的情况下，就可以避免读取所有数据。因此，对于 limit 较小的大数据查询，处理速度会更快。

该优化同时适用于 `ASC` 和 `DESC`，但不能与 [GROUP BY](/zh/sql-reference/statements/select/group-by) 子句和 [FINAL](/zh/sql-reference/statements/select/from#final-modifier) 修饰符一起使用。

禁用 `optimize_read_in_order` 设置时，ClickHouse server 在处理 `SELECT` 查询时不会使用表索引。

对于包含 `ORDER BY` 子句、较大的 `LIMIT`，以及在找到查询数据之前需要读取大量记录的 [WHERE](../../../sql-reference/statements/select/where.md) 条件的查询，可以考虑手动禁用 `optimize_read_in_order`。

以下表引擎支持此优化：

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (包括 [materialized views](/zh/sql-reference/statements/create/view#materialized-view)) ，
* [Merge](../../../engines/table-engines/special/merge.md)，
* [Buffer](../../../engines/table-engines/special/buffer.md)

在 `MaterializedView` 引擎表中，此优化适用于类似 `SELECT ... FROM merge_tree_table ORDER BY pk` 的视图。但对于类似 `SELECT ... FROM view ORDER BY pk` 的查询，如果该视图查询本身不包含 `ORDER BY` 子句，则不支持此优化。

<div id="order-by-expr-with-fill-modifier">
  ## ORDER BY Expr WITH FILL 修饰符
</div>

该修饰符也可与 [LIMIT ... WITH TIES 修饰符](/zh/sql-reference/statements/select/limit#limit--with-ties-modifier) 结合使用。

`WITH FILL` 修饰符可放在 `ORDER BY expr` 之后，并可选指定 `FROM expr`、`TO expr` 和 `STEP expr` 参数。
`expr` 列中所有缺失的值都会按顺序补齐，其他列则填充为默认值。

要填充多列，请在 `ORDER BY` 部分中每个字段名后添加带可选参数的 `WITH FILL` 修饰符。

```sql title="Query"
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

`WITH FILL` 可用于 Numeric (所有类型的 float、decimal、int) 或 Date/DateTime 类型的字段。用于 `String` 字段时，缺失值会填充为空字符串。
当未定义 `FROM const_expr` 时，填充序列将使用 `ORDER BY` 中 `expr` 字段的最小值。
当未定义 `TO const_expr` 时，填充序列将使用 `ORDER BY` 中 `expr` 字段的最大值。
当定义了 `STEP const_numeric_expr` 时，对于数值类型，`const_numeric_expr` 按原样解释；对于 Date 类型，解释为 `days`；对于 DateTime 类型，解释为 `seconds`。它还支持表示日期和时间间隔的 [INTERVAL](/zh/sql-reference/data-types/special-data-types/interval/) 数据类型。
当省略 `STEP const_numeric_expr` 时，填充序列对数值类型使用 `1.0`，对 Date 类型使用 `1 day`，对 DateTime 类型使用 `1 second`。
当定义了 `STALENESS const_numeric_expr` 时，查询将持续生成行，直到原始数据中与前一行的差值超过 `const_numeric_expr`。
`INTERPOLATE` 可用于未参与 `ORDER BY WITH FILL` 的列。这些列会基于前一个字段的值并应用 `expr` 进行填充。如果未提供 `expr`，则会重复前一个值。省略该列表将包含所有允许的列。

不使用 `WITH FILL` 的查询示例：

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

```text title="Response"
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

应用 `WITH FILL` 修饰符后的同一查询：

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┐
│   0 │          │
│ 0.5 │          │
│   1 │ original │
│ 1.5 │          │
│   2 │          │
│ 2.5 │          │
│   3 │          │
│ 3.5 │          │
│   4 │ original │
│ 4.5 │          │
│   5 │          │
│ 5.5 │          │
│   7 │ original │
└─────┴──────────┘
```

对于包含多个字段的情况，`ORDER BY field2 WITH FILL, field1 WITH FILL` 的填充顺序将遵循 `ORDER BY` 子句中字段的顺序。

示例：

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL,
    d1 WITH FILL STEP 5;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-01 │ 1970-01-03 │          │
│ 1970-01-01 │ 1970-01-04 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-01-01 │ 1970-01-06 │          │
│ 1970-01-01 │ 1970-01-07 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

字段 `d1` 不会被填充，而会使用默认值，因为 `d2` 的值没有重复项，因此无法正确计算 `d1` 的序列。

下面是修改了 `ORDER BY` 中字段后的查询：

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

以下查询对列 `d1` 中填充的每条数据使用了 1 天的 `INTERVAL` 数据类型：

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP INTERVAL 1 DAY,
    d2 WITH FILL;
```

```response title="Response"
┌─────────d1─┬─────────d2─┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-12 │ 1970-01-01 │          │
│ 1970-01-13 │ 1970-01-01 │          │
│ 1970-01-14 │ 1970-01-01 │          │
│ 1970-01-15 │ 1970-01-01 │          │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-17 │ 1970-01-01 │          │
│ 1970-01-18 │ 1970-01-01 │          │
│ 1970-01-19 │ 1970-01-01 │          │
│ 1970-01-20 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-22 │ 1970-01-01 │          │
│ 1970-01-23 │ 1970-01-01 │          │
│ 1970-01-24 │ 1970-01-01 │          │
│ 1970-01-25 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-27 │ 1970-01-01 │          │
│ 1970-01-28 │ 1970-01-01 │          │
│ 1970-01-29 │ 1970-01-01 │          │
│ 1970-01-30 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-01 │ 1970-01-01 │          │
│ 1970-02-02 │ 1970-01-01 │          │
│ 1970-02-03 │ 1970-01-01 │          │
│ 1970-02-04 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-06 │ 1970-01-01 │          │
│ 1970-02-07 │ 1970-01-01 │          │
│ 1970-02-08 │ 1970-01-01 │          │
│ 1970-02-09 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-11 │ 1970-01-01 │          │
│ 1970-02-12 │ 1970-01-01 │          │
│ 1970-02-13 │ 1970-01-01 │          │
│ 1970-02-14 │ 1970-01-01 │          │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-16 │ 1970-01-01 │          │
│ 1970-02-17 │ 1970-01-01 │          │
│ 1970-02-18 │ 1970-01-01 │          │
│ 1970-02-19 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-21 │ 1970-01-01 │          │
│ 1970-02-22 │ 1970-01-01 │          │
│ 1970-02-23 │ 1970-01-01 │          │
│ 1970-02-24 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-02-26 │ 1970-01-01 │          │
│ 1970-02-27 │ 1970-01-01 │          │
│ 1970-02-28 │ 1970-01-01 │          │
│ 1970-03-01 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-03 │ 1970-01-01 │          │
│ 1970-03-04 │ 1970-01-01 │          │
│ 1970-03-05 │ 1970-01-01 │          │
│ 1970-03-06 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-08 │ 1970-01-01 │          │
│ 1970-03-09 │ 1970-01-01 │          │
│ 1970-03-10 │ 1970-01-01 │          │
│ 1970-03-11 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

不使用 `STALENESS` 的查询示例：

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   3 │     0 │          │
 5. │   4 │     0 │          │
 6. │   5 │    25 │ original │
 7. │   6 │     0 │          │
 8. │   7 │     0 │          │
 9. │   8 │     0 │          │
10. │   9 │     0 │          │
11. │  10 │    50 │ original │
12. │  11 │     0 │          │
13. │  12 │     0 │          │
14. │  13 │     0 │          │
15. │  14 │     0 │          │
16. │  15 │    75 │ original │
    └─────┴───────┴──────────┘
```

应用 `STALENESS 3` 后的相同查询：

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   5 │    25 │ original │
 5. │   6 │     0 │          │
 6. │   7 │     0 │          │
 7. │  10 │    50 │ original │
 8. │  11 │     0 │          │
 9. │  12 │     0 │          │
10. │  15 │    75 │ original │
11. │  16 │     0 │          │
12. │  17 │     0 │          │
    └─────┴───────┴──────────┘
```

未使用 `INTERPOLATE` 的查询示例：

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     0 │
│   2 │          │     0 │
│ 2.5 │          │     0 │
│   3 │          │     0 │
│ 3.5 │          │     0 │
│   4 │ original │     4 │
│ 4.5 │          │     0 │
│   5 │          │     0 │
│ 5.5 │          │     0 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

应用 `INTERPOLATE` 后的同一查询：

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     2 │
│   2 │          │     3 │
│ 2.5 │          │     4 │
│   3 │          │     5 │
│ 3.5 │          │     6 │
│   4 │ original │     4 │
│ 4.5 │          │     5 │
│   5 │          │     6 │
│ 5.5 │          │     7 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

<div id="filling-grouped-by-sorting-prefix">
  ## 按排序前缀分组填充
</div>

对某些列中取值相同的行分别进行填充会很有用，时间序列中缺失值的填充就是一个很好的例子。
假设有如下时间序列表：

```sql
CREATE TABLE timeseries
(
    `sensor_id` UInt64,
    `timestamp` DateTime64(3, 'UTC'),
    `value` Float64
)
ENGINE = Memory;

SELECT * FROM timeseries;

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

我们希望按 1 秒的间隔，为每个传感器分别填充缺失值。
实现这一点的方法是，将 `sensor_id` 列作为填充列 `timestamp` 的排序前缀：

```sql
SELECT *
FROM timeseries
ORDER BY
    sensor_id,
    timestamp WITH FILL
INTERPOLATE ( value AS 9999 )

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       234 │ 2021-12-01 00:00:04.000 │  9999 │
│       234 │ 2021-12-01 00:00:05.000 │  9999 │
│       234 │ 2021-12-01 00:00:06.000 │  9999 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       432 │ 2021-12-01 00:00:02.000 │  9999 │
│       432 │ 2021-12-01 00:00:03.000 │  9999 │
│       432 │ 2021-12-01 00:00:04.000 │  9999 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

这里将 `value` 列插值为 `9999`，只是为了让填充出来的行更显眼。
此行为由 `use_with_fill_by_sorting_prefix` 设置控制 (默认启用)

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中处理时间序列数据和函数](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)