---
slug: /zh/sql-reference/functions/in-functions
sidebar_position: 60
sidebar_label: IN 运算符
---

# IN运算符相关函数 {#inyun-suan-fu-xiang-guan-han-shu}

## in,notIn,globalIn,globalNotIn {#in-notin-globalin-globalnotin}

请参阅[IN 运算符](../../sql-reference/operators/in.md#select-in-operators)部分。

## tuple(x, y, ...), 运算符 (x, y, ...) {#tuplex-y-operator-x-y}

函数用于对多个列进行分组。
对于具有类型T1，T2，...的列，它返回包含这些列的元组（T1，T2，...）。 执行该函数没有任何成本。
元组通常用作IN运算符的中间参数值，或用于创建lambda函数的形参列表。 元组不能写入表。

## tupleElement(tuple, n), 运算符 x.N {#tupleelementtuple-n-operator-x-n}

用于从元组中获取列的函数
’N’是列索引，从1开始。N必须是正整数常量，并且不大于元组的大小。
执行该函数没有任何成本。
