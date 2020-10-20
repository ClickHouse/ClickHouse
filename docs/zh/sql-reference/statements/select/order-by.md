---
toc_title: ORDER BY
---

# ORDER BY {#select-order-by}

`ORDER BY` 子句包含一个表达式列表，每个表达式都可以用 `DESC` （降序）或 `ASC` （升序）修饰符确定排序方向。 如果未指定方向, 默认是 `ASC` ，所以它通常被省略。 排序方向适用于单个表达式，而不适用于整个列表。 示例: `ORDER BY Visits DESC, SearchPhrase`

对于排序表达式列表具有相同值的行以任意顺序输出，也可以是非确定性的（每次都不同）。
如果省略ORDER BY子句，则行的顺序也是未定义的，并且可能也是非确定性的。

## 特殊值的排序 {#sorting-of-special-values}

有两种方法 `NaN` 和 `NULL` 排序顺序:

-   默认情况下或与 `NULLS LAST` 修饰符：首先是值，然后 `NaN`，然后 `NULL`.
-   与 `NULLS FIRST` 修饰符：第一 `NULL`，然后 `NaN`，然后其他值。

### 示例 {#example}

对于表

``` text
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

运行查询 `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` 获得:

``` text
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

当对浮点数进行排序时，Nan与其他值是分开的。 无论排序顺序如何，Nan都在最后。 换句话说，对于升序排序，它们被放置为好像它们比所有其他数字大，而对于降序排序，它们被放置为好像它们比其他数字小。

## 排序规则支持 {#collation-support}

对于按字符串值排序，可以指定排序规则（比较）。 示例: `ORDER BY SearchPhrase COLLATE 'tr'` -对于按关键字升序排序，使用土耳其字母，不区分大小写，假设字符串是UTF-8编码。 `COLLATE` 可以按顺序独立地指定或不按每个表达式。 如果 `ASC` 或 `DESC` 被指定, `COLLATE` 在它之后指定。 使用时 `COLLATE`，排序始终不区分大小写。

我们只建议使用 `COLLATE` 对于少量行的最终排序，因为排序与 `COLLATE` 比正常的按字节排序效率低。

## 实现细节 {#implementation-details}

更少的RAM使用，如果一个足够小 [LIMIT](../../../sql-reference/statements/select/limit.md) 除了指定 `ORDER BY`. 否则，所花费的内存量与用于排序的数据量成正比。 对于分布式查询处理，如果 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 省略排序，在远程服务器上部分完成排序，并将结果合并到请求者服务器上。 这意味着对于分布式排序，要排序的数据量可以大于单个服务器上的内存量。

如果没有足够的RAM，则可以在外部存储器中执行排序（在磁盘上创建临时文件）。 使用设置 `max_bytes_before_external_sort` 为此目的。 如果将其设置为0（默认值），则禁用外部排序。 如果启用，则当要排序的数据量达到指定的字节数时，将对收集的数据进行排序并转储到临时文件中。 读取所有数据后，将合并所有已排序的文件并输出结果。 文件被写入到 `/var/lib/clickhouse/tmp/` 目录中的配置（默认情况下，但你可以使用 `tmp_path` 参数来更改此设置）。

运行查询可能占用的内存比 `max_bytes_before_external_sort` 大. 因此，此设置的值必须大大小于 `max_memory_usage`. 例如，如果您的服务器有128GB的RAM，并且您需要运行单个查询，请设置 `max_memory_usage` 到100GB，和 `max_bytes_before_external_sort` 至80GB。

外部排序的工作效率远远低于在RAM中进行排序。

## ORDER BY Expr WITH FILL Modifier {#orderby-with-fill}

此修饰符可以与 [LIMIT … WITH TIES modifier](../../../sql-reference/statements/select/limit.md#limit-with-ties) 进行组合使用.

可以在`ORDER BY expr`之后用可选的`FROM expr`，`TO expr`和`STEP expr`参数来设置`WITH FILL`修饰符。
所有`expr`列的缺失值将被顺序填充，而其他列将被填充为默认值。

使用以下语法填充多列，在ORDER BY部分的每个字段名称后添加带有可选参数的WITH FILL修饰符。

``` sql
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr]
```

`WITH FILL` 仅适用于具有数字（所有类型的浮点，小数，整数）或日期/日期时间类型的字段。
当未定义 `FROM const_expr` 填充顺序时，则使用 `ORDER BY` 中的最小 `expr` 字段值。
如果未定义 `TO const_expr` 填充顺序，则使用 `ORDER BY` 中的最大`expr`字段值。
当定义了 `STEP const_numeric_expr` 时，对于数字类型，`const_numeric_expr` 将 `as is` 解释为 `days` 作为日期类型，将 `seconds` 解释为DateTime类型。
如果省略了 `STEP const_numeric_expr`，则填充顺序使用 `1.0` 表示数字类型，`1 day`表示日期类型，`1 second` 表示日期时间类型。

例如下面的查询：

``` sql
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n
```

返回

``` text
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

但是如果配置了 `WITH FILL` 修饰符

``` sql
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5
```

返回

``` text
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

For the case when we have multiple fields `ORDER BY field2 WITH FILL, field1 WITH FILL` order of filling will follow the order of fields in `ORDER BY` clause.
对于我们有多个字段 `ORDER BY field2 WITH FILL, field1 WITH FILL ` 的情况，填充顺序将遵循` ORDER BY`子句中字段的顺序。

示例:

``` sql
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

返回

``` text
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

字段 `d1` 没有填充并使用默认值，因为我们没有 `d2` 值的重复值，并且无法正确计算 `d1` 的顺序。
以下查询中`ORDER BY` 中的字段将被更改

``` sql
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

返回

``` text
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
