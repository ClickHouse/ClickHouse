# system.data_type_families {#system_tables-data_type_families}

Содержит информацию о поддерживаемых [типах данных](../../sql-reference/data-types/).

Столбцы:

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя типа данных.
-   `case_insensitive` ([UInt8](../../sql-reference/data-types/int-uint.md)) — свойство, которое показывает, зависит ли имя типа данных в запросе от регистра. Например, допустимы и `Date`, и `date`.
-   `alias_to` ([String](../../sql-reference/data-types/string.md)) — тип данных, для которого `name` является алиасом.

**Пример**

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

**See Also**

-   [Синтаксис](../../sql-reference/syntax.md) — поддерживаемый SQL синтаксис.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/data_type_families) <!--hide-->
