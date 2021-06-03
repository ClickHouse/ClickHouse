# system.replicated_fetches {#system_tables-replicated_fetches}

Содержит информацию о выполняемых в данный момент фоновых операциях скачивания кусков данных с других реплик.

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.

-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.

-   `elapsed` ([Float64](../../sql-reference/data-types/float.md)) — время, прошедшее от момента начала скачивания куска, в секундах.

-   `progress` ([Float64](../../sql-reference/data-types/float.md)) — доля выполненной работы от 0 до 1.

-   `result_part_name` ([String](../../sql-reference/data-types/string.md)) — имя скачиваемого куска.

-   `result_part_path` ([String](../../sql-reference/data-types/string.md)) — абсолютный путь к скачиваемому куску.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — идентификатор партиции.

-   `total_size_bytes_compressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер сжатой информации в скачиваемом куске в байтах.

-   `bytes_read_compressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер сжатой информации, считанной из скачиваемого куска, в байтах.

-   `source_replica_path` ([String](../../sql-reference/data-types/string.md)) — абсолютный путь к исходной реплике.

-   `source_replica_hostname` ([String](../../sql-reference/data-types/string.md)) — имя хоста исходной реплики.

-   `source_replica_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — номер порта исходной реплики.

-   `interserver_scheme` ([String](../../sql-reference/data-types/string.md)) — имя межсерверной схемы.

-   `URI` ([String](../../sql-reference/data-types/string.md)) — универсальный идентификатор ресурса.

-   `to_detached` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, указывающий на использование выражения `TO DETACHED` в текущих фоновых операциях.

-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — идентификатор потока.

**Пример**

``` sql
SELECT * FROM system.replicated_fetches LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:                    default
table:                       t
elapsed:                     7.243039876
progress:                    0.41832135995612835
result_part_name:            all_0_0_0
result_part_path:            /var/lib/clickhouse/store/700/70080a04-b2de-4adf-9fa5-9ea210e81766/all_0_0_0/
partition_id:                all
total_size_bytes_compressed: 1052783726
bytes_read_compressed:       440401920
source_replica_path:         /clickhouse/test/t/replicas/1
source_replica_hostname:     node1
source_replica_port:         9009
interserver_scheme:          http
URI:                         http://node1:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftest%2Ft%2Freplicas%2F1&part=all_0_0_0&client_protocol_version=4&compress=false
to_detached:                 0
thread_id:                   54
```

**Смотрите также**

-   [Управление таблицами ReplicatedMergeTree](../../sql-reference/statements/system/#query-language-system-replicated)

[Оригинальная статья](https://clickhouse.tech/docs/en/operations/system_tables/replicated_fetches) <!--hide-->
