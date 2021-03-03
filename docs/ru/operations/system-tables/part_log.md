# system.part_log {#system_tables-part-log}

Системная таблица `system.part_log` создается только в том случае, если задана серверная настройка [part_log](../server-configuration-parameters/settings.md#server_configuration_parameters-part-log).

Содержит информацию о всех событиях, произошедших с [кусками данных](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) (например, события добавления, удаления или слияния данных).

Столбцы:

-   `event_type` (Enum) — тип события. Столбец может содержать одно из следующих значений:
    -   `NEW_PART` — вставка нового куска.
    -   `MERGE_PARTS` — слияние кусков.
    -   `DOWNLOAD_PART` — загрузка с реплики.
    -   `REMOVE_PART` — удаление или отсоединение из таблицы с помощью [DETACH PARTITION](../../sql-reference/statements/alter/partition.md#alter_detach-partition).
    -   `MUTATE_PART` — изменение куска.
    -   `MOVE_PART` — перемещение куска между дисками.
-   `event_date` (Date) — дата события.
-   `event_time` (DateTime) — время события.
-   `duration_ms` (UInt64) — длительность.
-   `database` (String) — имя базы данных, в которой находится кусок.
-   `table` (String) — имя таблицы, в которой находится кусок.
-   `part_name` (String) — имя куска.
-   `partition_id` (String) — идентификатор партиции, в которую был добавлен кусок. В столбце будет значение ‘all’, если таблица партициируется по выражению `tuple()`.
-   `rows` (UInt64) — число строк в куске.
-   `size_in_bytes` (UInt64) — размер куска данных в байтах.
-   `merged_from` (Array(String)) — массив имён кусков, из которых образован текущий кусок в результате слияния (также столбец заполняется в случае скачивания уже смерженного куска).
-   `bytes_uncompressed` (UInt64) — количество прочитанных разжатых байт.
-   `read_rows` (UInt64) — сколько было прочитано строк при слиянии кусков.
-   `read_bytes` (UInt64) — сколько было прочитано байт при слиянии кусков.
-   `error` (UInt16) — код ошибки, возникшей при текущем событии.
-   `exception` (String) — текст ошибки.

Системная таблица `system.part_log` будет создана после первой вставки данных в таблицу `MergeTree`.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/part_log) <!--hide-->
