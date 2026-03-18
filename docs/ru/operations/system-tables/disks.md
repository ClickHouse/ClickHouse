---
slug: /ru/operations/system-tables/disks
---
# system.disks {#system_tables-disks}

Cодержит информацию о дисках, заданных в [конфигурации сервера](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Столбцы:

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя диска в конфигурации сервера.
-   `path` ([String](../../sql-reference/data-types/string.md)) — путь к точке монтирования в файловой системе.
-   `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — свободное место на диске в байтах.
-   `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — объём диска в байтах.
-   `unreserved_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — не зарезервированное cвободное место в байтах (`free_space` минус размер места, зарезервированного на  выполняемые в данный момент фоновые слияния, вставки и другие операции записи на диск).
-   `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — место, которое должно остаться свободным на диске в байтах. Задаётся значением параметра `keep_free_space_bytes` конфигурации дисков.
