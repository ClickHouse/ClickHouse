---
slug: /ru/sql-reference/table-functions/cluster
sidebar_position: 50
sidebar_label: cluster
---

# cluster, clusterAllReplicas {#cluster-clusterallreplicas}

Позволяет обратиться ко всем шардам существующего кластера, который сконфигурирован в секции `remote_servers` без создания таблицы типа [Distributed](../../engines/table-engines/special/distributed.md). В запросе используется одна реплика каждого шарда.

Функция `clusterAllReplicas` работает также как `cluster`, но каждая реплика в кластере используется как отдельный шард/отдельное соединение.

:::note Примечание
Все доступные кластеры перечислены в таблице [system.clusters](../../operations/system-tables/clusters.md).
:::
**Синтаксис**

``` sql
cluster('cluster_name', db.table[, sharding_key])
cluster('cluster_name', db, table[, sharding_key])
clusterAllReplicas('cluster_name', db.table[, sharding_key])
clusterAllReplicas('cluster_name', db, table[, sharding_key])
```
**Аргументы**

- `cluster_name` – имя кластера, который обозначает подмножество адресов и параметров подключения к удаленным и локальным серверам, входящим в кластер.
- `db.table` или `db`, `table` - имя базы данных и таблицы. 
- `sharding_key` - ключ шардирования. Необязательный аргумент. Указывается, если данные добавляются более чем в один шард кластера. 

**Возвращаемое значение**

Набор данных из кластеров.

**Использование макросов**

`cluster_name` может содержать макрос — подстановку в фигурных скобках. Эта подстановка заменяется на соответствующее значение из секции [macros](../../operations/server-configuration-parameters/settings.md#macros) конфигурационного файла.

Пример:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

**Использование и рекомендации** 

Использование табличных функций `cluster` и `clusterAllReplicas` менее оптимально, чем создание таблицы типа `Distributed`, поскольку в этом случае при каждом новом запросе устанавливается новое соединение с сервером. При обработке большого количества запросов всегда создавайте `Distributed` таблицу заранее и не используйте табличные функции `cluster` и `clusterAllReplicas`.

Табличные функции `cluster` and `clusterAllReplicas` могут быть полезны в следующих случаях:

-   Чтение данных из конкретного кластера для сравнения данных, отладки и тестирования.
-   Запросы к разным ClickHouse кластерам и репликам в целях исследования.
-   Нечастых распределенных запросов которые делаются вручную.

Настройки соединения `user`, `password`, `host`, `post`, `compression`, `secure` берутся из секции `<remote_servers>` файлов конфигурации. См. подробности в разделе [Distributed](../../engines/table-engines/special/distributed.md)

**См. также**

-   [skip_unavailable_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)
-   [load_balancing](../../operations/settings/settings.md#settings-load_balancing)
