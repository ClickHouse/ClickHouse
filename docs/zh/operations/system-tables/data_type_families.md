---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。data\_type\_families {#system_tables-data_type_families}

包含有关受支持的信息 [数据类型](../../sql-reference/data-types/).

列:

-   `name` ([字符串](../../sql-reference/data-types/string.md)) — Data type name.
-   `case_insensitive` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Property that shows whether you can use a data type name in a query in case insensitive manner or not. For example, `Date` 和 `date` 都是有效的。
-   `alias_to` ([字符串](../../sql-reference/data-types/string.md)) — Data type name for which `name` 是个化名

**示例**

``` sql
SELECT * FROM system.data_type_families WHERE alias_to = 'String'
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

-   [语法](../../sql-reference/syntax.md) — Information about supported syntax.
