# system.tables {#system-tables}

Содержит метаданные каждой таблицы, о которой знает сервер. 

Отсоединённые таблицы ([DETACH](../../sql-reference/statements/detach.md)) не отображаются в `system.tables`.

Информация о [временных таблицах](../../sql-reference/statements/create/table.md#temporary-tables) содержится в `system.tables` только в тех сессиях, в которых эти таблицы были созданы. Поле `database` у таких таблиц пустое, а флаг `is_temporary` включен. 

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.
-   `name` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `engine` ([String](../../sql-reference/data-types/string.md)) — движок таблицы (без параметров).
-   `is_temporary` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, указывающий на то, временная это таблица или нет.
-   `data_path` ([String](../../sql-reference/data-types/string.md)) — путь к данным таблицы в файловой системе.
-   `metadata_path` ([String](../../sql-reference/data-types/string.md)) — путь к табличным метаданным в файловой системе.
-   `metadata_modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время последней модификации табличных метаданных.
-   `dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — зависимости базы данных.
-   `dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — табличные зависимости (таблицы [MaterializedView](../../engines/table-engines/special/materializedview.md), созданные на базе текущей таблицы).
-   `create_table_query` ([String](../../sql-reference/data-types/string.md)) — запрос, при помощи которого создавалась таблица.
-   `engine_full` ([String](../../sql-reference/data-types/string.md)) — параметры табличного движка.
-   `partition_key` ([String](../../sql-reference/data-types/string.md)) — ключ партиционирования таблицы.
-   `sorting_key` ([String](../../sql-reference/data-types/string.md)) — ключ сортировки таблицы.
-   `primary_key` ([String](../../sql-reference/data-types/string.md)) - первичный ключ таблицы.
-   `sampling_key` ([String](../../sql-reference/data-types/string.md)) — ключ сэмплирования таблицы.
-   `storage_policy` ([String](../../sql-reference/data-types/string.md)) - политика хранения данных:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distributed](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - общее количество строк, если есть возможность быстро определить точное количество строк в таблице, в противном случае `NULL` (включая базовую таблицу `Buffer`).

-   `total_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - общее количество байт, если можно быстро определить точное количество байт для таблицы на накопителе, в противном случае `NULL` (не включает в себя никакого базового хранилища).

    -   Если таблица хранит данные на диске, возвращает используемое пространство на диске (т. е. сжатое).
    -   Если таблица хранит данные в памяти, возвращает приблизительное количество используемых байт в памяти.

-   `lifetime_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - общее количество строк, добавленных оператором `INSERT` с момента запуска сервера (только для таблиц `Buffer`).

-   `lifetime_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - общее количество байт, добавленных оператором `INSERT` с момента запуска сервера (только для таблиц `Buffer`).

Таблица `system.tables` используется при выполнении запроса `SHOW TABLES`.

**Пример**

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   system
name:                       aggregate_function_combinators
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     SystemAggregateFunctionCombinators
is_temporary:               0
data_paths:                 []
metadata_path:              /var/lib/clickhouse/metadata/system/aggregate_function_combinators.sql
metadata_modification_time: 1970-01-01 03:00:00
dependencies_database:      []
dependencies_table:         []
create_table_query:         
engine_full:                
partition_key:              
sorting_key:                
primary_key:                
sampling_key:               
storage_policy:             
total_rows:                 ᴺᵁᴸᴸ
total_bytes:                ᴺᵁᴸᴸ

Row 2:
──────
database:                   system
name:                       asynchronous_metrics
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     SystemAsynchronousMetrics
is_temporary:               0
data_paths:                 []
metadata_path:              /var/lib/clickhouse/metadata/system/asynchronous_metrics.sql
metadata_modification_time: 1970-01-01 03:00:00
dependencies_database:      []
dependencies_table:         []
create_table_query:         
engine_full:                
partition_key:              
sorting_key:                
primary_key:                
sampling_key:               
storage_policy:             
total_rows:                 ᴺᵁᴸᴸ
total_bytes:                ᴺᵁᴸᴸ
```
