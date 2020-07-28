---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: ORDER BY
---

# 按条款订购 {#select-order-by}

该 `ORDER BY` 子句包含一个表达式列表，每个表达式都可以用 `DESC` （降序）或 `ASC` （升序）修饰符确定排序方向。 如果未指定方向, `ASC` 假设，所以它通常被省略。 排序方向适用于单个表达式，而不适用于整个列表。 示例: `ORDER BY Visits DESC, SearchPhrase`

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

## 实施细节 {#implementation-details}

更少的RAM使用，如果一个足够小 [LIMIT](../../../sql-reference/statements/select/limit.md) 除了指定 `ORDER BY`. 否则，所花费的内存量与用于排序的数据量成正比。 对于分布式查询处理，如果 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 省略排序，在远程服务器上部分完成排序，并将结果合并到请求者服务器上。 这意味着对于分布式排序，要排序的数据量可以大于单个服务器上的内存量。

如果没有足够的RAM，则可以在外部存储器中执行排序（在磁盘上创建临时文件）。 使用设置 `max_bytes_before_external_sort` 为此目的。 如果将其设置为0（默认值），则禁用外部排序。 如果启用，则当要排序的数据量达到指定的字节数时，将对收集的数据进行排序并转储到临时文件中。 读取所有数据后，将合并所有已排序的文件并输出结果。 文件被写入到 `/var/lib/clickhouse/tmp/` 目录中的配置（默认情况下，但你可以使用 `tmp_path` 参数来更改此设置）。

运行查询可能占用的内存比 `max_bytes_before_external_sort`. 因此，此设置的值必须大大小于 `max_memory_usage`. 例如，如果您的服务器有128GB的RAM，并且您需要运行单个查询，请设置 `max_memory_usage` 到100GB，和 `max_bytes_before_external_sort` 至80GB。

外部排序的工作效率远远低于在RAM中进行排序。
