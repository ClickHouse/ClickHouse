
# system.data_type_families {#system_tables-data_type_families}

包含有关受支持的[数据类型](../../sql-reference/data-types/)的信息.

列字段包括:

-   `name` ([String](../../sql-reference/data-types/string.md)) — 数据类型的名称.
-   `case_insensitive` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 该属性显示是否可以在查询中以不区分大小写的方式使用数据类型名称。例如 `Date` 和 `date` 都是有效的。
-   `alias_to` ([String](../../sql-reference/data-types/string.md)) — 名称为别名的数据类型名称。

**示例**

``` sql
SELECT * FROM system.data_type_families WHERE alias_to = 'String';
```

``` text
┌─name───────┬─case_insensitive─┬─alias_to─┐
│ LONGBLOB   │                1 │ String   │
│ LONGTEXT   │                1 │ String   │
│ TINYTEXT   │                1 │ String   │
│ TEXT       │                1 │ String   │
│ VARCHAR    │                1 │ String   │
│ MEDIUMBLOB │                1 │ String   │
│ BLOB       │                1 │ String   │
│ TINYBLOB   │                1 │ String   │
│ CHAR       │                1 │ String   │
│ MEDIUMTEXT │                1 │ String   │
└────────────┴──────────────────┴──────────┘
```

**另请参阅**

-   [Syntax](../../sql-reference/syntax.md) — 关于所支持的语法信息.
