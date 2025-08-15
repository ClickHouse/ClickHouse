---
slug: /ru/operations/system-tables/replication_queue
---
# system.replication_queue {#system_tables-replication_queue}

Содержит информацию о задачах из очередей репликации, хранящихся в ZooKeeper, для таблиц семейства `ReplicatedMergeTree`.

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.

-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.

-   `replica_name` ([String](../../sql-reference/data-types/string.md)) — имя реплики в ZooKeeper. Разные реплики одной и той же таблицы имеют различные имена.

-   `position` ([UInt32](../../sql-reference/data-types/int-uint.md)) — позиция задачи в очереди.

-   `node_name` ([String](../../sql-reference/data-types/string.md)) — имя узла в ZooKeeper.

-   `type` ([String](../../sql-reference/data-types/string.md)) — тип задачи в очереди:

    -   `GET_PART` — скачать кусок с другой реплики.
    -   `ATTACH_PART` — присоединить кусок. Задача может быть выполнена и с куском из нашей собственной реплики (если он находится в папке `detached`). Эта задача практически идентична задаче `GET_PART`, лишь немного оптимизирована.
    -   `MERGE_PARTS` — выполнить слияние кусков.
    -   `DROP_RANGE` — удалить куски в партициях из указнного диапазона.
    -   `CLEAR_COLUMN` — удалить указанный столбец из указанной партиции. Примечание: не используется с 20.4.
    -   `CLEAR_INDEX` — удалить указанный индекс из указанной партиции. Примечание: не используется с 20.4.
    -   `REPLACE_RANGE` — удалить указанный диапазон кусков и заменить их на новые.
    -   `MUTATE_PART` — применить одну или несколько мутаций к куску.
    -   `ALTER_METADATA` — применить изменения структуры таблицы в результате запросов с выражением `ALTER`.

-   `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время отправки задачи на выполнение.

-   `required_quorum` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество реплик, ожидающих завершения задачи, с подтверждением о завершении. Этот столбец актуален только для задачи `GET_PARTS`.

-   `source_replica` ([String](../../sql-reference/data-types/string.md)) — имя исходной реплики.

-   `new_part_name` ([String](../../sql-reference/data-types/string.md)) — имя нового куска.

-   `parts_to_merge` ([Array](../../sql-reference/data-types/array.md) ([String](../../sql-reference/data-types/string.md))) — имена кусков, которые требуется смержить или обновить.

-   `is_detach` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, указывающий на присутствие в очереди задачи `DETACH_PARTS`.

-   `is_currently_executing` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, указывающий на выполнение конкретной задачи на данный момент.

-   `num_tries` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество неудачных попыток выполнить задачу.

-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — текст сообщения о последней возникшей ошибке, если таковые имеются.

-   `last_attempt_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время последней попытки выполнить задачу.

-   `num_postponed` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество откладываний запуска задачи.

-   `postpone_reason` ([String](../../sql-reference/data-types/string.md)) — причина, по которой была отложена задача.

-   `last_postpone_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время, когда была отложена задача в последний раз.

-   `merge_type` ([String](../../sql-reference/data-types/string.md)) — тип текущего слияния. Пусто, если это мутация.

**Пример**

``` sql
SELECT * FROM system.replication_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:               merge
table:                  visits_v2
replica_name:           mtgiga001-1t.metrika.yandex.net
position:               15
node_name:              queue-0009325559
type:                   MERGE_PARTS
create_time:            2020-12-07 14:04:21
required_quorum:        0
source_replica:         mtgiga001-1t.metrika.yandex.net
new_part_name:          20201130_121373_121384_2
parts_to_merge:         ['20201130_121373_121378_1','20201130_121379_121379_0','20201130_121380_121380_0','20201130_121381_121381_0','20201130_121382_121382_0','20201130_121383_121383_0','20201130_121384_121384_0']
is_detach:              0
is_currently_executing: 0
num_tries:              36
last_exception:         Code: 226, e.displayText() = DB::Exception: Marks file '/opt/clickhouse/data/merge/visits_v2/tmp_fetch_20201130_121373_121384_2/CounterID.mrk' doesn't exist (version 20.8.7.15 (official build))
last_attempt_time:      2020-12-08 17:35:54
num_postponed:          0
postpone_reason:
last_postpone_time:     1970-01-01 03:00:00
```

**Смотрите также**

-   [Управление таблицами ReplicatedMergeTree](../../sql-reference/statements/system.md#query-language-system-replicated)
