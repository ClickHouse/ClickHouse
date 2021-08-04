---
toc_priority: 47
toc_title: OPTIMIZE
---

# OPTIMIZE {#misc_operations-optimize}

Запрос пытается запустить внеплановое слияние кусков данных для таблиц.

!!! warning "Внимание"
    `OPTIMIZE` не устраняет причину появления ошибки `Too many parts`.

**Синтаксис**

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
```

Может применяться к таблицам семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), [MaterializedView](../../engines/table-engines/special/materializedview.md) и [Buffer](../../engines/table-engines/special/buffer.md). Другие движки таблиц не поддерживаются.

Если запрос `OPTIMIZE` применяется к таблицам семейства [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md), ClickHouse создаёт задачу на слияние и ожидает её исполнения на всех узлах (если активирована настройка `replication_alter_partitions_sync`).

-   По умолчанию, если запросу `OPTIMIZE` не удалось выполнить слияние, то
ClickHouse не оповещает клиента. Чтобы включить оповещения, используйте настройку [optimize_throw_if_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop).
-   Если указать `PARTITION`, то оптимизация выполняется только для указанной партиции. [Как задавать имя партиции в запросах](alter/index.md#alter-how-to-specify-part-expr).
-   Если указать `FINAL`, то оптимизация выполняется даже в том случае, если все данные уже лежат в одном куске данных. Кроме того, слияние является принудительным, даже если выполняются параллельные слияния.
-   Если указать `DEDUPLICATE`, то произойдет схлопывание полностью одинаковых строк (сравниваются значения во всех столбцах), имеет смысл только для движка MergeTree.

## Выражение BY {#by-expression}

Чтобы выполнить дедупликацию по произвольному набору столбцов, вы можете явно указать список столбцов или использовать любую комбинацию подстановки [`*`](../../sql-reference/statements/select/index.md#asterisk), выражений [`COLUMNS`](../../sql-reference/statements/select/index.md#columns-expression) и [`EXCEPT`](../../sql-reference/statements/select/index.md#except-modifier).

 Список столбцов для дедупликации должен включать все столбцы, указанные в условиях сортировки (первичный ключ и ключ сортировки), а также в условиях партиционирования (ключ партиционирования).

 !!! note "Примечание"
    Обратите внимание, что символ подстановки `*` обрабатывается так же, как и в запросах `SELECT`: столбцы `MATERIALIZED` и `ALIAS` не включаются в результат.
    Если указать пустой список или выражение, которое возвращает пустой список, или дедуплицировать столбец по псевдониму (`ALIAS`), то сервер вернет ошибку.


**Примеры**

Рассмотрим таблицу:

``` sql
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key;
```

Прежний способ дедупликации, когда учитываются все столбцы. Строка удаляется только в том случае, если все значения во всех столбцах равны соответствующим значениям в предыдущей строке.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

Дедупликация по всем столбцам, кроме `ALIAS` и `MATERIALIZED`: `primary_key`, `secondary_key`, `value`, `partition_key` и `materialized_value`.


``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

Дедупликация по всем столбцам, кроме `ALIAS`, `MATERIALIZED` и `materialized_value`: столбцы `primary_key`, `secondary_key`, `value` и `partition_key`.


``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT materialized_value;
```

Дедупликация по столбцам `primary_key`, `secondary_key` и `partition_key`.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

Дедупликация по любому столбцу, соответствующему регулярному выражению: столбцам `primary_key`, `secondary_key` и `partition_key`.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```
