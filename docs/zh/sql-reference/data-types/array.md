# 阵列(T) {#data-type-array}

由 `T` 类型元素组成的数组。

`T` 可以是任意类型，包含数组类型。 但不推荐使用多维数组，ClickHouse 对多维数组的支持有限。例如，不能存储在 `MergeTree` 表中存储多维数组。

## 创建数组 {#chuang-jian-shu-zu}

您可以使用array函数来创建数组：

    array(T)

您也可以使用方括号：

    []

创建数组示例：

    :) SELECT array(1, 2) AS x, toTypeName(x)

    SELECT
        [1, 2] AS x,
        toTypeName(x)

    ┌─x─────┬─toTypeName(array(1, 2))─┐
    │ [1,2] │ Array(UInt8)            │
    └───────┴─────────────────────────┘

    1 rows in set. Elapsed: 0.002 sec.

    :) SELECT [1, 2] AS x, toTypeName(x)

    SELECT
        [1, 2] AS x,
        toTypeName(x)

    ┌─x─────┬─toTypeName([1, 2])─┐
    │ [1,2] │ Array(UInt8)       │
    └───────┴────────────────────┘

    1 rows in set. Elapsed: 0.002 sec.

## 使用数据类型 {#shi-yong-shu-ju-lei-xing}

ClickHouse会自动检测数组元素,并根据元素计算出存储这些元素最小的数据类型。如果在元素中存在 [NULL](../../sql-reference/data-types/array.md#null-literal) 或存在 [可为空](nullable.md#data_type-nullable) 类型元素，那么数组的元素类型将会变成 [可为空](nullable.md)。

如果 ClickHouse 无法确定数据类型，它将产生异常。当尝试同时创建一个包含字符串和数字的数组时会发生这种情况 (`SELECT array(1, 'a')`)。

自动数据类型检测示例：

    :) SELECT array(1, 2, NULL) AS x, toTypeName(x)

    SELECT
        [1, 2, NULL] AS x,
        toTypeName(x)

    ┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
    │ [1,2,NULL] │ Array(Nullable(UInt8))        │
    └────────────┴───────────────────────────────┘

    1 rows in set. Elapsed: 0.002 sec.

如果您尝试创建不兼容的数据类型数组，ClickHouse 将引发异常：

    :) SELECT array(1, 'a')

    SELECT [1, 'a']

    Received exception from server (version 1.1.54388):
    Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.

    0 rows in set. Elapsed: 0.246 sec.
