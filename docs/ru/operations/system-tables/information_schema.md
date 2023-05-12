# INFORMATION_SCHEMA {#information-schema}

`INFORMATION_SCHEMA` (`information_schema`) — это системная база данных, содержащая представления. Используя эти представления, вы можете получить информацию о метаданных объектов базы данных. Эти представления считывают данные из столбцов системных таблиц [system.columns](../../operations/system-tables/columns.md), [system.databases](../../operations/system-tables/databases.md) и [system.tables](../../operations/system-tables/tables.md).

Структура и состав системных таблиц могут меняться в разных версиях СУБД ClickHouse, но поддержка `information_schema` позволяет изменять структуру системных таблиц без изменения способа доступа к метаданным. Запросы метаданных не зависят от используемой СУБД.

``` sql
SHOW TABLES FROM INFORMATION_SCHEMA;
```

``` text
┌─name─────┐
│ COLUMNS  │
│ SCHEMATA │
│ TABLES   │
│ VIEWS    │
└──────────┘
```

`INFORMATION_SCHEMA` содержит следующие представления:

-   [COLUMNS](#columns)
-   [SCHEMATA](#schemata)
-   [TABLES](#tables)
-   [VIEWS](#views)

## COLUMNS {#columns}

Содержит столбцы, которые считываются из системной таблицы [system.columns](../../operations/system-tables/columns.md), и столбцы, которые не поддерживаются в ClickHouse или не имеют смысла (всегда имеют значение `NULL`), но должны быть по стандарту.

Столбцы:

-   `table_catalog` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `table_schema` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `table_name` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `column_name` ([String](../../sql-reference/data-types/string.md)) — имя столбца.
-   `ordinal_position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — порядковый номер столбца в таблице (нумерация начинается с 1).
-   `column_default` ([String](../../sql-reference/data-types/string.md)) — выражение для значения по умолчанию или пустая строка.
-   `is_nullable` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий является ли столбец типа `Nullable`.
-   `data_type` ([String](../../sql-reference/data-types/string.md)) — тип столбца.
-   `character_maximum_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальная длина в байтах для двоичных данных, символьных данных или текстовых данных и изображений. В ClickHouse имеет смысл только для типа данных `FixedString`. Иначе возвращается значение `NULL`.
-   `character_octet_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальная длина в байтах для двоичных данных, символьных данных или текстовых данных и изображений. В ClickHouse имеет смысл только для типа данных `FixedString`. Иначе возвращается значение `NULL`.
-   `numeric_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — точность приблизительных числовых данных, точных числовых данных, целочисленных данных или денежных данных. В ClickHouse это разрядность для целочисленных типов и десятичная точность для типов `Decimal`. Иначе возвращается значение `NULL`.
-   `numeric_precision_radix` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — основание системы счисления точности приблизительных числовых данных, точных числовых данных, целочисленных данных или денежных данных. В ClickHouse значение столбца равно 2 для целочисленных типов и 10 — для типов `Decimal`. Иначе возвращается значение `NULL`.
-   `numeric_scale` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — масштаб приблизительных числовых данных, точных числовых данных, целочисленных данных или денежных данных. В ClickHouse имеет смысл только для типов `Decimal`. Иначе возвращается значение `NULL`.
-   `datetime_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — десятичная точность для данных типа `DateTime64`. Для других типов данных возвращается значение `NULL`.
-   `character_set_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `character_set_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `character_set_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `collation_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `collation_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `collation_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `domain_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `domain_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `domain_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.

**Пример**

Запрос:

``` sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema=currentDatabase() OR table_schema='') AND table_name NOT LIKE '%inner%' LIMIT 1 FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
table_catalog:            default
table_schema:             default
table_name:               describe_example
column_name:              id
ordinal_position:         1
column_default:
is_nullable:              0
data_type:                UInt64
character_maximum_length: ᴺᵁᴸᴸ
character_octet_length:   ᴺᵁᴸᴸ
numeric_precision:        64
numeric_precision_radix:  2
numeric_scale:            0
datetime_precision:       ᴺᵁᴸᴸ
character_set_catalog:    ᴺᵁᴸᴸ
character_set_schema:     ᴺᵁᴸᴸ
character_set_name:       ᴺᵁᴸᴸ
collation_catalog:        ᴺᵁᴸᴸ
collation_schema:         ᴺᵁᴸᴸ
collation_name:           ᴺᵁᴸᴸ
domain_catalog:           ᴺᵁᴸᴸ
domain_schema:            ᴺᵁᴸᴸ
domain_name:              ᴺᵁᴸᴸ
```

## SCHEMATA {#schemata}

Содержит столбцы, которые считываются из системной таблицы [system.databases](../../operations/system-tables/databases.md), и столбцы, которые не поддерживаются в ClickHouse или не имеют смысла (всегда имеют значение `NULL`), но должны быть по стандарту.

Столбцы:

-   `catalog_name` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.
-   `schema_name` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.
-   `schema_owner` ([String](../../sql-reference/data-types/string.md)) — имя владельца схемы, всегда `'default'`.
-   `default_character_set_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `default_character_set_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `default_character_set_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.
-   `sql_path` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, не поддерживается.

**Пример**

Запрос:

``` sql
SELECT * FROM information_schema.schemata WHERE schema_name ILIKE 'information_schema' LIMIT 1 FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
catalog_name:                  INFORMATION_SCHEMA
schema_name:                   INFORMATION_SCHEMA
schema_owner:                  default
default_character_set_catalog: ᴺᵁᴸᴸ
default_character_set_schema:  ᴺᵁᴸᴸ
default_character_set_name:    ᴺᵁᴸᴸ
sql_path:                      ᴺᵁᴸᴸ
```

## TABLES {#tables}

Содержит столбцы, которые считываются из системной таблицы [system.tables](../../operations/system-tables/tables.md).

Столбцы:

-   `table_catalog` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `table_schema` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `table_name` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `table_type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип таблицы. Возможные значения:
    -   `BASE TABLE`
    -   `VIEW`
    -   `FOREIGN TABLE`
    -   `LOCAL TEMPORARY`
    -   `SYSTEM VIEW`

**Пример**

Запрос:

``` sql
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%' LIMIT 1 FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
table_catalog: default
table_schema:  default
table_name:    describe_example
table_type:    BASE TABLE
```

## VIEWS {#views}

Содержит столбцы, которые считываются из системной таблицы [system.tables](../../operations/system-tables/tables.md), если использован движок [View](../../engines/table-engines/special/view.md).

Столбцы:

-   `table_catalog` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `table_schema` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `table_name` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `view_definition` ([String](../../sql-reference/data-types/string.md)) — `SELECT` запрос для представления.
-   `check_option` ([String](../../sql-reference/data-types/string.md)) — `NONE`, нет проверки.
-   `is_updatable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, представление не обновляется.
-   `is_insertable_into` ([Enum8](../../sql-reference/data-types/enum.md)) — показывает является ли представление [материализованным](../../sql-reference/statements/create/view/#materialized). Возможные значения:
    -   `NO` — создано обычное представление.
    -   `YES` — создано материализованное представление.
-   `is_trigger_updatable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, триггер не обновляется.
-   `is_trigger_deletable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, триггер не удаляется.
-   `is_trigger_insertable_into` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, данные не вставляются в триггер.

**Пример**

Запрос:

``` sql
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
CREATE MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM system.one;
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase() LIMIT 1 FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
table_catalog:              default
table_schema:               default
table_name:                 mv
view_definition:            SELECT * FROM system.one
check_option:               NONE
is_updatable:               NO
is_insertable_into:         YES
is_trigger_updatable:       NO
is_trigger_deletable:       NO
is_trigger_insertable_into: NO
```
