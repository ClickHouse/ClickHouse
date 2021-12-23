# system.columns {#system-columns}

Содержит информацию о столбцах всех таблиц.

С помощью этой таблицы можно получить информацию аналогично запросу [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table), но для многих таблиц сразу.

Колонки [временных таблиц](../../sql-reference/statements/create/table.md#temporary-tables) содержатся в `system.columns` только в тех сессиях, в которых эти таблицы были созданы. Поле `database` у таких колонок пустое.

Cтолбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.
-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `name` ([String](../../sql-reference/data-types/string.md)) — имя столбца.
-   `type` ([String](../../sql-reference/data-types/string.md)) — тип столбца.
-   `position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — порядковый номер столбца в таблице (нумерация начинается с 1).
-   `default_kind` ([String](../../sql-reference/data-types/string.md)) — тип выражения (`DEFAULT`, `MATERIALIZED`, `ALIAS`) для значения по умолчанию или пустая строка.
-   `default_expression` ([String](../../sql-reference/data-types/string.md)) — выражение для значения по умолчанию или пустая строка.
-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер сжатых данных в байтах.
-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер распакованных данных в байтах.
-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер засечек в байтах.
-   `comment` ([String](../../sql-reference/data-types/string.md)) — комментарий к столбцу или пустая строка.
-   `is_in_partition_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в ключ партиционирования.
-   `is_in_sorting_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в ключ сортировки.
-   `is_in_primary_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в первичный ключ.
-   `is_in_sampling_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в ключ выборки.
-   `compression_codec` ([String](../../sql-reference/data-types/string.md)) — имя кодека сжатия.
-   `character_octet_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальная длина в байтах для двоичных данных, символьных данных или текстовых данных и изображений. В ClickHouse имеет смысл только для типа данных `FixedString`. Иначе возвращается значение `NULL`.
-   `numeric_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — точность приблизительных числовых данных, точных числовых данных, целочисленных данных или денежных данных. В ClickHouse это разрядность для целочисленных типов и десятичная точность для типов `Decimal`. Иначе возвращается значение `NULL`.
-   `numeric_precision_radix` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — основание системы счисления точности приблизительных числовых данных, точных числовых данных, целочисленных данных или денежных данных. В ClickHouse значение столбца равно 2 для целочисленных типов и 10 — для типов `Decimal`. Иначе возвращается значение `NULL`.
-   `numeric_scale` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — масштаб приблизительных числовых данных, точных числовых данных, целочисленных данных или денежных данных. В ClickHouse имеет смысл только для типов `Decimal`. Иначе возвращается значение `NULL`.
-   `datetime_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — десятичная точность для данных типа `DateTime64`. Для других типов данных возвращается значение `NULL`.

**Пример**

```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_catalog
type:                    String
position:                1
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ

Row 2:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_schema
type:                    String
position:                2
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ
```
