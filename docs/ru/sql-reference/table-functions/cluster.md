---
toc_priority: 50
toc_title: cluster
---

# cluster, clusterAllReplicas {#cluster-clusterallreplicas}

Позволяет обратиться ко всем серверам существующего кластера, который присутствует в таблице `system.clusters` и сконфигурирован в секцци `remote_servers` без создания таблицы типа `Distributed`.
`clusterAllReplicas` - работает также как `cluster` но каждая реплика в кластере будет использована как отдельный шард/отдельное соединение. 


Сигнатуры:

``` sql
cluster('cluster_name', db.table)
cluster('cluster_name', db, table)
clusterAllReplicas('cluster_name', db.table)
clusterAllReplicas('cluster_name', db, table)
```

`cluster_name` – имя кластера, который обязан присутствовать в таблице `system.clusters`  и обозначает подмножество адресов и параметров подключения к удаленным и локальным серверам, входящим в кластер. 

Использование табличных функций `cluster` и `clusterAllReplicas` менее оптимальное чем создание таблицы типа `Distributed`, поскольку в этом случае соединение с сервером переустанавливается на каждый запрос. При обработке большого количества запросов, всегда создавайте `Distributed` таблицу заранее и не используйте табличные функции `cluster` и `clusterAllReplicas`.

Табличные функции `cluster` and `clusterAllReplicas` могут быть полезны в следующих случаях:

-   Чтение данных из конкретного кластера для сравнения данных, отладки и тестирования.
-   Запросы к разным ClickHouse кластерам и репликам в целях исследования.
-   Нечастых распределенных запросов которые делаются вручную.

Настройки соединения `user`, `password`, `host`, `post`, `compression`, `secure` берутся из секции `<remote_servers>` файлов конфигурации. См. подробности в разделе [Distributed](../../engines/table-engines/special/distributed.md)

**See Also**

-   [skip\_unavailable\_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)
-   [load\_balancing](../../operations/settings/settings.md#settings-load_balancing)
