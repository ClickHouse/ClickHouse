# system.data_skipping_indices {#system-data-skipping-indices}

Содержит информацию о существующих индексах пропуска данных во всех таблицах.

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.
-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `name` ([String](../../sql-reference/data-types/string.md)) — имя индекса.
-   `type` ([String](../../sql-reference/data-types/string.md)) — тип индекса.
-   `expr` ([String](../../sql-reference/data-types/string.md)) — выражение, используемое для вычисления индекса.
-   `granularity` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество гранул в блоке данных.
-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер сжатых данных в байтах.
-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер несжатых данных в байтах.
-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер засечек в байтах.

**Пример**

```sql
SELECT * FROM system.data_skipping_indices LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       user_actions
name:        clicks_idx
type:        minmax
expr:        clicks
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks:                   48

Row 2:
──────
database:    default
table:       users
name:        contacts_null_idx
type:        minmax
expr:        assumeNotNull(contacts_null)
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks:                   48
```
