# system.tables {#system-tables}

Содержит метаданные каждой таблицы, о которой знает сервер. Отсоединённые таблицы не отображаются в `system.tables`.

Эта таблица содержит следующие столбцы (тип столбца показан в скобках):

-   `database String` — имя базы данных, в которой находится таблица.
-   `name` (String) — имя таблицы.
-   `engine` (String) — движок таблицы (без параметров).
-   `is_temporary` (UInt8) — флаг, указывающий на то, временная это таблица или нет.
-   `data_path` (String) — путь к данным таблицы в файловой системе.
-   `metadata_path` (String) — путь к табличным метаданным в файловой системе.
-   `metadata_modification_time` (DateTime) — время последней модификации табличных метаданных.
-   `dependencies_database` (Array(String)) — зависимости базы данных.
-   `dependencies_table` (Array(String)) — табличные зависимости (таблицы [MaterializedView](../../engines/table-engines/special/materializedview.md), созданные на базе текущей таблицы).
-   `create_table_query` (String) — запрос, которым создавалась таблица.
-   `engine_full` (String) — параметры табличного движка.
-   `partition_key` (String) — ключ партиционирования таблицы.
-   `sorting_key` (String) — ключ сортировки таблицы.
-   `primary_key` (String) - первичный ключ таблицы.
-   `sampling_key` (String) — ключ сэмплирования таблицы.
-   `storage_policy` (String) - политика хранения данных:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distributed](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64)) - общее количество строк, если есть возможность быстро определить точное количество строк в таблице, в противном случае `Null` (включая базовую таблицу `Buffer`).

-   `total_bytes` (Nullable(UInt64)) - общее количество байт, если можно быстро определить точное количество байт для таблицы на накопителе, в противном случае `Null` (**не включает** в себя никакого базового хранилища).

    -   Если таблица хранит данные на диске, возвращает используемое пространство на диске (т. е. сжатое).
    -   Если таблица хранит данные в памяти, возвращает приблизительное количество используемых байт в памяти.

-   `lifetime_rows` (Nullable(UInt64)) - общее количество строк, добавленных оператором `INSERT` с момента запуска сервера (только для таблиц `Buffer`).

-   `lifetime_bytes` (Nullable(UInt64)) - общее количество байт, добавленных оператором `INSERT` с момента запуска сервера (только для таблиц `Buffer`).

Таблица `system.tables` используется при выполнении запроса `SHOW TABLES`.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/tables) <!--hide-->
