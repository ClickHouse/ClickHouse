# system.replicas

Содержит информацию и статус для реплицируемых таблиц, расположенных на локальном сервере.
Эту таблицу можно использовать для мониторинга. Таблица содержит по строчке для каждой Replicated\*-таблицы.

Пример:

```sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

```text
Row 1:
──────
database:           merge
table:              visits
engine:             ReplicatedCollapsingMergeTree
is_leader:          1
is_readonly:        0
is_session_expired: 0
future_parts:       1
parts_to_check:     0
zookeeper_path:     /clickhouse/tables/01-06/visits
replica_name:       example01-06-1.yandex.ru
replica_path:       /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:    9
queue_size:         1
inserts_in_queue:   0
merges_in_queue:    1
log_max_index:      596273
log_pointer:        596274
total_replicas:     2
active_replicas:    2
```

Столбцы:

```text
database:           имя БД
table:              имя таблицы
engine:             имя движка таблицы

is_leader:          является ли реплика лидером

В один момент времени, не более одной из реплик является лидером. Лидер отвечает за выбор фоновых слияний, которые следует произвести.
Замечу, что запись можно осуществлять на любую реплику (доступную и имеющую сессию в ZK), независимо от лидерства.

is_readonly:        находится ли реплика в режиме "только для чтения"
Этот режим включается, если в конфиге нет секции с ZK; если при переинициализации сессии в ZK произошла неизвестная ошибка; во время переинициализации сессии с ZK.

is_session_expired: истекла ли сессия с ZK.
В основном, то же самое, что и is_readonly.

future_parts:       количество кусков с данными, которые появятся в результате INSERT-ов или слияний, которых ещё предстоит сделать

parts_to_check:     количество кусков с данными в очереди на проверку
Кусок помещается в очередь на проверку, если есть подозрение, что он может быть битым.

zookeeper_path:     путь к данным таблицы в ZK
replica_name:       имя реплики в ZK; разные реплики одной таблицы имеют разное имя
replica_path:       путь к данным реплики в ZK. То же самое, что конкатенация zookeeper_path/replicas/replica_path.

columns_version:    номер версии структуры таблицы
Обозначает, сколько раз был сделан ALTER. Если на репликах разные версии, значит некоторые реплики сделали ещё не все ALTER-ы.

queue_size:         размер очереди действий, которых предстоит сделать
К действиям относятся вставки блоков данных, слияния, и некоторые другие действия.
Как правило, совпадает с future_parts.

inserts_in_queue:   количество вставок блоков данных, которых предстоит сделать
Обычно вставки должны быстро реплицироваться. Если величина большая - значит что-то не так.

merges_in_queue:    количество слияний, которых предстоит сделать
Бывают длинные слияния - то есть, это значение может быть больше нуля продолжительное время.

Следующие 4 столбца имеют ненулевое значение только если активна сессия с ZK.

log_max_index:      максимальный номер записи в общем логе действий
log_pointer:        максимальный номер записи из общего лога действий, которую реплика скопировала в свою очередь для выполнения, плюс единица
Если log_pointer сильно меньше log_max_index, значит что-то не так.

total_replicas:     общее число известных реплик этой таблицы
active_replicas:    число реплик этой таблицы, имеющих сессию в ZK; то есть, число работающих реплик
```

Если запрашивать все столбцы, то таблица может работать слегка медленно, так как на каждую строчку делается несколько чтений из ZK.
Если не запрашивать последние 4 столбца (log_max_index, log_pointer, total_replicas, active_replicas), то таблица работает быстро.

Например, так можно проверить, что всё хорошо:

```sql
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
