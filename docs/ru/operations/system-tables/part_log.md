# system.part_log {#system_tables-part-log}

Системная таблица `system.part_log` создается только в том случае, если задана серверная настройка [part_log](../server-configuration-parameters/settings.md#server_configuration_parameters-part-log).

Содержит информацию о всех событиях, произошедших с [кусками данных](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) (например, события добавления, удаления или слияния данных).

Столбцы:

-   `query_id` ([String](../../sql-reference/data-types/string.md)) — идентификатор запроса `INSERT`, создавшего этот кусок.
-   `event_type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип события. Столбец может содержать одно из следующих значений:
    -   `NEW_PART` — вставка нового куска.
    -   `MERGE_PARTS` — слияние кусков.
    -   `DOWNLOAD_PART` — загрузка с реплики.
    -   `REMOVE_PART` — удаление или отсоединение из таблицы с помощью [DETACH PARTITION](../../sql-reference/statements/alter/partition.md#alter_detach-partition).
    -   `MUTATE_PART` — изменение куска.
    -   `MOVE_PART` — перемещение куска между дисками.
-   `merge_reason` ([Enum8](../../sql-reference/data-types/enum.md)) — Причина события с типом `MERGE_PARTS`. Может принимать одно из следующих значений:
    -   `NOT_A_MERGE` — событие имеет тип иной, чем `MERGE_PARTS`.
    -   `REGULAR_MERGE` — обычное слияние.
    -   `TTL_DELETE_MERGE` — очистка истекших данных.
    -   `TTL_RECOMPRESS_MERGE` — переупаковка куска.
-   `merge_algorithm` ([Enum8](../../sql-reference/data-types/enum.md)) — Алгоритм слияния для события с типом `MERGE_PARTS`. Может принимать одно из следующих значений:
    -   `UNDECIDED`
    -   `HORIZONTAL`
    -   `VERTICAL`
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата события.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время события.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время события с точностью до микросекунд.
-   `duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — длительность.
-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится кусок.
-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы, в которой находится кусок.
-   `part_name` ([String](../../sql-reference/data-types/string.md)) — имя куска.
-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — идентификатор партиции, в которую был добавлен кусок. В столбце будет значение `all`, если таблица партициируется по выражению `tuple()`.
-   `path_on_disk` ([String](../../sql-reference/data-types/string.md)) — абсолютный путь к папке с файлами кусков данных.
-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — число строк в куске.
-   `size_in_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер куска данных в байтах.
-   `merged_from` ([Array(String)](../../sql-reference/data-types/array.md)) — массив имён кусков, из которых образован текущий кусок в результате слияния (также столбец заполняется в случае скачивания уже смерженного куска).
-   `bytes_uncompressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество прочитанных не сжатых байт.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — сколько было прочитано строк при слиянии кусков.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — сколько было прочитано байт при слиянии кусков.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — максимальная разница между выделенной и освобождённой памятью в контексте потока.
-   `error` ([UInt16](../../sql-reference/data-types/int-uint.md)) — код ошибки, возникшей при текущем событии.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — текст ошибки.

Системная таблица `system.part_log` будет создана после первой вставки данных в таблицу `MergeTree`.

**Пример**

``` sql
SELECT * FROM system.part_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
query_id:                      983ad9c7-28d5-4ae1-844e-603116b7de31
event_type:                    NewPart
merge_reason:                  NotAMerge
merge_algorithm:               Undecided
event_date:                    2021-02-02
event_time:                    2021-02-02 11:14:28
event_time_microseconds:                    2021-02-02 11:14:28.861919
duration_ms:                   35
database:                      default
table:                         log_mt_2
part_name:                     all_1_1_0
partition_id:                  all
path_on_disk:                  db/data/default/log_mt_2/all_1_1_0/
rows:                          115418
size_in_bytes:                 1074311
merged_from:                   []
bytes_uncompressed:            0
read_rows:                     0
read_bytes:                    0
peak_memory_usage:             0
error:                         0
exception:
```

