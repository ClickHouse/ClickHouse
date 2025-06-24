---
slug: /ru/operations/system-tables/tables
---
# system.tables {#system-tables}

Содержит метаданные каждой таблицы, о которой знает сервер.

Отсоединённые таблицы ([DETACH](../../sql-reference/statements/detach.md)) не отображаются в `system.tables`.

Информация о [временных таблицах](../../sql-reference/statements/create/table.md#temporary-tables) содержится в `system.tables` только в тех сессиях, в которых эти таблицы были созданы. Поле `database` у таких таблиц пустое, а флаг `is_temporary` включен.

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится таблица.

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.

-   `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Uuid таблицы (Atomic database).

-   `engine` ([String](../../sql-reference/data-types/string.md)) — движок таблицы (без параметров).

-   `is_temporary` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, указывающий на то, временная это таблица или нет.

-   `data_paths` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — пути к данным таблицы в файловых системах.

-   `metadata_path` ([String](../../sql-reference/data-types/string.md)) — путь к табличным метаданным в файловой системе.

-   `metadata_modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время последней модификации табличных метаданных.

-   `dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — зависимости базы данных.

-   `dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — табличные зависимости (таблицы [MaterializedView](../../engines/table-engines/special/materializedview.md), созданные на базе текущей таблицы).

-   `create_table_query` ([String](../../sql-reference/data-types/string.md)) — запрос, при помощи которого создавалась таблица.

-   `engine_full` ([String](../../sql-reference/data-types/string.md)) — параметры табличного движка.

-   `as_select` ([String](../../sql-reference/data-types/string.md)) - `SELECT` запрос для представления.

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

-   `has_own_data` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий хранит ли таблица сама какие-то данные на диске или только обращается к какому-то другому источнику.

-   `loading_dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - базы данных необходимые для загрузки объекта.

-   `loading_dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - таблицы необходимые для загрузки объекта.

-   `loading_dependent_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - базы данных, которым объект необходим для загрузки.

-   `loading_dependent_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - таблицы, которым объект необходим для загрузки.

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
as_select:                  SELECT database AS table_catalog
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
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []

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
as_select:                  SELECT name AS catalog_name
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
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []
```
