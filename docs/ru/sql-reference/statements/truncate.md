---
toc_priority: 51
toc_title: TRUNCATE
---

# TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет все данные из таблицы. Если условие `IF EXISTS` не указано, запрос вернет ошибку, если таблицы не существует.

Запрос `TRUNCATE` не поддерживается для следующих движков: [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md), [Buffer](../../engines/table-engines/special/buffer.md) и [Null](../../engines/table-engines/special/null.md).

Вы можете настроить ожидание выполнения действий на репликах с помощью настройки [replication_alter_partitions_sync](../../operations/settings/settings.md#replication-alter-partitions-sync).

Вы можете указать время ожидания (в секундах) выполнения запросов `TRUNCATE` для неактивных реплик с помощью настройки [replication_wait_for_inactive_replica_timeout](../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout).

!!! info "Примечание"
    Если значение настройки `replication_alter_partitions_sync` равно `2` и некоторые реплики не активны больше времени, заданного настройкой `replication_wait_for_inactive_replica_timeout`, то генерируется исключение `UNFINISHED`.
