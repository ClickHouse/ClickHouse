# system.replicas {#system_tables-replicas}

Содержит информацию и статус для реплицируемых таблиц, расположенных на локальном сервере.
Эту таблицу можно использовать для мониторинга. Таблица содержит по строчке для каждой Replicated\*-таблицы.

Пример:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'test_table'
FORMAT Vertical
```

``` text
Query id: dc6dcbcb-dc28-4df9-ae27-4354f5b3b13e

Row 1:
───────
database:                    db
table:                       test_table
engine:                      ReplicatedMergeTree
is_leader:                   1
can_become_leader:           1
is_readonly:                 0
is_session_expired:          0
future_parts:                0
parts_to_check:              0
zookeeper_path:              /test/test_table
replica_name:                r1
replica_path:                /test/test_table/replicas/r1
columns_version:             -1
queue_size:                  27
inserts_in_queue:            27
merges_in_queue:             0
part_mutations_in_queue:     0
queue_oldest_time:           2021-10-12 14:48:48
inserts_oldest_time:         2021-10-12 14:48:48
merges_oldest_time:          1970-01-01 03:00:00
part_mutations_oldest_time:  1970-01-01 03:00:00
oldest_part_to_get:          1_17_17_0
oldest_part_to_merge_to:     
oldest_part_to_mutate_to:    
log_max_index:               206
log_pointer:                 207
last_queue_update:           2021-10-12 14:50:08
absolute_delay:              99
total_replicas:              5
active_replicas:             5
last_queue_update_exception: 
zookeeper_exception:         
replica_is_active:           {'r1':1,'r2':1}
```

Столбцы:

-   `database` (`String`) - имя БД.
-   `table` (`String`) - имя таблицы.
-   `engine` (`String`) - имя движка таблицы.
-   `is_leader` (`UInt8`) - является ли реплика лидером.
    Несколько реплик могут быть лидерами одновременно. Реплике можно запретить быть лидером с помощью `merge_tree` настройки `replicated_can_become_leader`. Лидеры назначают фоновые слияния, которые следует произвести.
    Замечу, что запись можно осуществлять на любую реплику (доступную и имеющую сессию в ZK), независимо от лидерства.
-   `can_become_leader` (`UInt8`) - может ли реплика быть лидером.
-   `is_readonly` (`UInt8`) - находится ли реплика в режиме «только для чтения»
    Этот режим включается, если в конфиге нет секции с ZK; если при переинициализации сессии в ZK произошла неизвестная ошибка; во время переинициализации сессии с ZK.
-   `is_session_expired` (`UInt8`) - истекла ли сессия с ZK. В основном, то же самое, что и `is_readonly`.
-   `future_parts` (`UInt32`) - количество кусков с данными, которые появятся в результате INSERT-ов или слияний, которых ещё предстоит сделать
-   `parts_to_check` (`UInt32`) - количество кусков с данными в очереди на проверку. Кусок помещается в очередь на проверку, если есть подозрение, что он может быть битым.
-   `zookeeper_path` (`String`) - путь к данным таблицы в ZK.
-   `replica_name` (`String`) - имя реплики в ZK; разные реплики одной таблицы имеют разное имя.
-   `replica_path` (`String`) - путь к данным реплики в ZK. То же самое, что конкатенация zookeeper_path/replicas/replica_path.
-   `columns_version` (`Int32`) - номер версии структуры таблицы. Обозначает, сколько раз был сделан ALTER. Если на репликах разные версии, значит некоторые реплики сделали ещё не все ALTER-ы.
-   `queue_size` (`UInt32`) - размер очереди действий, которые предстоит сделать. К действиям относятся вставки блоков данных, слияния, и некоторые другие действия. Как правило, совпадает с future_parts.
-   `inserts_in_queue` (`UInt32`) - количество вставок блоков данных, которые предстоит сделать. Обычно вставки должны быстро реплицироваться. Если величина большая - значит что-то не так.
-   `merges_in_queue` (`UInt32`) - количество слияний, которые предстоит сделать. Бывают длинные слияния - то есть, это значение может быть больше нуля продолжительное время.
-   `part_mutations_in_queue` (`UInt32`) - количество мутаций, которые предстоит сделать.
-   `queue_oldest_time` (`DateTime`) - если `queue_size` больше 0, показывает, когда была добавлена в очередь самая старая операция.
-   `inserts_oldest_time` (`DateTime`) - см. `queue_oldest_time`.
-   `merges_oldest_time` (`DateTime`) - см. `queue_oldest_time`.
-   `part_mutations_oldest_time` (`DateTime`) - см. `queue_oldest_time`.

Следующие 4 столбца имеют ненулевое значение только если активна сессия с ZK.

-   `log_max_index` (`UInt64`) - максимальный номер записи в общем логе действий.
-   `log_pointer` (`UInt64`) - максимальный номер записи из общего лога действий, которую реплика скопировала в свою очередь для выполнения, плюс единица. Если log_pointer сильно меньше log_max_index, значит что-то не так.
-   `last_queue_update` (`DateTime`) - время последнего обновления запроса.
-   `absolute_delay` (`UInt64`) - задержка (в секундах) для текущей реплики.
-   `total_replicas` (`UInt8`) - общее число известных реплик этой таблицы.
-   `active_replicas` (`UInt8`) - число реплик этой таблицы, имеющих сессию в ZK; то есть, число работающих реплик.
-   `last_queue_update_exception` (`String`) - если в очереди есть битые записи. Особенно важно, когда в ClickHouse нарушается обратная совместимость между версиями, а записи журнала, сделанные более новыми версиями, не могут быть проанализированы старыми версиями.
-   `zookeeper_exception` (`String`) - последнее сообщение об исключении. Появляется, если ошибка произошла при получении информации из ZooKeeper.
-   `replica_is_active` ([Map(String, UInt8)](../../sql-reference/data-types/map.md)) — соответствие между именем реплики и признаком активности реплики.

Если запрашивать все столбцы, то таблица может работать слегка медленно, так как на каждую строчку делается несколько чтений из ZK.
Если не запрашивать последние 4 столбца (log_max_index, log_pointer, total_replicas, active_replicas), то таблица работает быстро.

Например, так можно проверить, что всё хорошо:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

Если этот запрос ничего не возвращает - значит всё хорошо.


