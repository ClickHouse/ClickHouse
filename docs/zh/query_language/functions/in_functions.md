# IN运算符相关函数

## in, notIn, globalIn, globalNotIn

请参阅[IN 运算符](../select.md#select-in-operators)部分。

## tuple(x, y, ...), operator (x, y, ...)

函数用于对多个列进行分组。
对于具有类型T1，T2，...的列，它返回包含这些列的元组（T1，T2，...）。 执行该函数没有任何成本。
元组通常用作IN运算符的中间参数值，或用于创建lambda函数的形参列表。 元组不能写入表。

## tupleElement(tuple, n), operator x.N

函数用于从元组中获取列。
'N'是列索引，从1开始。N必须是常量正整数常数，并且不大于元组的大小。
执行该函数没有任何成本。


[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/in_functions/) <!--hide-->
