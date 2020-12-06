---
toc_priority: 47
toc_title: OPTIMIZE
---

# OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

Запрос пытается запустить внеплановый мёрж кусков данных для таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Другие движки таблиц не поддерживаются.

Если `OPTIMIZE` применяется к таблицам семейства [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md), ClickHouse создаёт задачу на мёрж и ожидает её исполнения на всех узлах (если активирована настройка `replication_alter_partitions_sync`).

-   Если `OPTIMIZE` не выполняет мёрж по любой причине, ClickHouse не оповещает об этом клиента. Чтобы включить оповещения, используйте настройку [optimize_throw_if_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop).
-   Если указать `PARTITION`, то оптимизация выполняется только для указанной партиции. [Как задавать имя партиции в запросах](alter/index.md#alter-how-to-specify-part-expr).
-   Если указать `FINAL`, то оптимизация выполняется даже в том случае, если все данные уже лежат в одном куске.
-   Если указать `DEDUPLICATE`, то произойдет схлопывание полностью одинаковых строк (сравниваются значения во всех колонках), имеет смысл только для движка MergeTree.

!!! warning "Внимание"
    Запрос `OPTIMIZE` не может устранить причину появления ошибки «Too many parts».

    
[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/optimize/) <!--hide-->