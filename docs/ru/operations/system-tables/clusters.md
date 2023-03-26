# system.clusters {#system-clusters}

Содержит информацию о доступных в конфигурационном файле кластерах и серверах, которые в них входят.

Столбцы:

-   `cluster` ([String](../../sql-reference/data-types/string.md)) — имя кластера.
-   `shard_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — номер шарда в кластере, начиная с 1.
-   `shard_weight` ([UInt32](../../sql-reference/data-types/int-uint.md)) — относительный вес шарда при записи данных.
-   `replica_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — номер реплики в шарде, начиная с 1.
-   `host_name` ([String](../../sql-reference/data-types/string.md)) — хост, указанный в конфигурации.
-   `host_address` ([String](../../sql-reference/data-types/string.md)) — TIP-адрес хоста, полученный из DNS.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт для соединения с сервером.
-   `is_local` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий является ли хост локальным.
-   `user` ([String](../../sql-reference/data-types/string.md)) — имя пользователя для соединения с сервером.
-   `default_database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных по умолчанию.
-   `errors_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество неудачных попыток хоста получить доступ к реплике.
-   `slowdowns_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество замен реплики из-за долгого отсутствия ответа от нее при установке соединения при хеджированных запросах.
-   `estimated_recovery_time` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество секунд до момента, когда количество ошибок будет обнулено и реплика станет доступной.

**Пример**

Запрос:

```sql
SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

Результат:

```text
Row 1:
──────
cluster:                 test_cluster_two_shards
shard_num:               1
shard_weight:            1
replica_num:             1
host_name:               127.0.0.1
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0

Row 2:
──────
cluster:                 test_cluster_two_shards
shard_num:               2
shard_weight:            1
replica_num:             1
host_name:               127.0.0.2
host_address:            127.0.0.2
port:                    9000
is_local:                0
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
```

**Смотрите также**

-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [Настройка distributed_replica_error_cap](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [Настройка distributed_replica_error_half_life](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)

[Оригинальная статья](https://clickhouse.com/docs/ru/operations/system_tables/clusters) <!--hide-->
