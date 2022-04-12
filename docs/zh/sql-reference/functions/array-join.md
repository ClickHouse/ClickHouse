# arrayJoin函数 {#functions_arrayjoin}

这是一个非常有用的函数。

普通函数不会更改结果集的行数，而只是计算每行中的值（map）。
聚合函数将多行压缩到一行中（fold或reduce）。
’arrayJoin’函数获取每一行并将他们展开到多行（unfold）。

此函数将数组作为参数，并将该行在结果集中复制数组元素个数。
除了应用此函数的列中的值之外，简单地复制列中的所有值;它被替换为相应的数组值。

查询可以使用多个`arrayJoin`函数。在这种情况下，转换被执行多次。

请注意SELECT查询中的ARRAY JOIN语法，它提供了更广泛的可能性。

示例:

``` sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

    ┌─dst─┬─\'Hello\'─┬─src─────┐
    │   1 │ Hello     │ [1,2,3] │
    │   2 │ Hello     │ [1,2,3] │
    │   3 │ Hello     │ [1,2,3] │
    └─────┴───────────┴─────────┘

[来源文章](https://clickhouse.com/docs/en/query_language/functions/array_join/) <!--hide-->
