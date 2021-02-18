---
title: Возможно ли удалить старые записи из таблицы ClickHouse?
toc_hidden: true
toc_priority: 20
---

# Возможно ли удалить старые записи из таблицы ClickHouse? {#is-it-possible-to-delete-old-records-from-a-clickhouse-table}

Если отвечать коротко, то да. В ClickHouse есть множество механизмов, которые позволяют освобождать место на диске, удаляя старые данные. Каждый механизм подходит для разных сценариев.

## TTL {#ttl}

ClickHouse позволяет автоматически сбрасывать значения при выполнении некоторого условия. Это условие конфигурируется как выражение на основании любых столбцов, обычно это статическое смещение allows to automatically drop values when some condition happens. This condition is configured as an expression based on any columns, usually just static offset for any timestamp column.

Ключевое преимущество такого подхода в том, что не нужно никакой внешней системы для тригера, The key advantage of this approach is that it doesn’t need any external system to trigger, once TTL is configured, data removal happens automatically in background.

!!! note "Note"
    TTL можно использовать не только для перемещения на [/dev/null](https://en.wikipedia.org/wiki/Null_device), но еще и между системами хранения, например, с SSD на HDD.

[Подробнее о конфигурировании TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

## ALTER DELETE {#alter-delete}

ClickHouse doesn’t have real-time point deletes like in [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) databases. The closest thing to them are mutations. They are issued as `ALTER ... DELETE` or `ALTER ... UPDATE` queries to distinguish from normal `DELETE` or `UPDATE` as they are asynchronous batch operations, not immediate modifications. The rest of syntax after `ALTER TABLE` prefix is similar.

`ALTER DELETE` can be issued to flexibly remove old data. If you need to do it regularly, the main downside will be the need to have an external system to submit the query. There are also some performance considerations since mutation rewrite complete parts even there’s only a single row to be deleted.

Это самый распространенный подход к тому, чтобы сделать вашу систему на CH отвечающей принципам [GDPR](https://gdpr-info.eu).

More details on [mutations](../../sql-reference/statements/alter/index.md#alter-mutations).

## DROP PARTITION {#drop-partition}

`ALTER TABLE ... DROP PARTITION` provides a cost-efficient way to drop a whole partition. It’s not that flexible and needs proper partitioning scheme configured on table creation, but still covers most common cases. Like mutations need to be executed from an external system for regular use.

More details on [manipulating partitions](../../sql-reference/statements/alter/partition.md#alter_drop-partition).

## TRUNCATE {#truncate}

Достаточно радикальный способ — очищать всю таблицу от данных, но очень подходящий в отдельных случаях.

More details on [table truncation](../../sql-reference/statements/alter/partition.md#alter_drop-partition).
