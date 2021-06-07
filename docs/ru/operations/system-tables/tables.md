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

-   `comment` ([String](../../sql-reference/data-types/string.md)) — комментарий к таблице.

Таблица `system.tables` используется при выполнении запроса `SHOW TABLES`.

**Пример**

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
name:                       t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/store/81b/81b1c20a-b7c6-4116-a2ce-7583fb6b6736/']
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
metadata_modification_time: 2021-01-25 19:14:32
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE base.t1 (`n` UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 8192
engine_full:                MergeTree ORDER BY n SETTINGS index_granularity = 8192
partition_key:
sorting_key:                n
primary_key:                n
sampling_key:
storage_policy:             default
total_rows:                 1
total_bytes:                99
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:

Row 2:
──────
database:                   default
name:                       53r93yleapyears
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/data/default/53r93yleapyears/']
metadata_path:              /var/lib/clickhouse/metadata/default/53r93yleapyears.sql
metadata_modification_time: 2020-09-23 09:05:36
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE default.`53r93yleapyears` (`id` Int8, `febdays` Int8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192
engine_full:                MergeTree ORDER BY id SETTINGS index_granularity = 8192
partition_key:
sorting_key:                id
primary_key:                id
sampling_key:
storage_policy:             default
total_rows:                 2
total_bytes:                155
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
```
