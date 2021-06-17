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
    Обратите внимание, что символ подстановки `*` обрабатывается так же, как и в запросах `SELECT`: столбцы [MATERIALIZED](../../sql-reference/statements/create/table.md#materialized) и [ALIAS](../../sql-reference/statements/create/table.md#alias) не включаются в результат.
    Если указать пустой список или выражение, которое возвращает пустой список, то сервер вернет ошибку. Запрос вида `DEDUPLICATE BY aliased_value` также вернет ошибку.

**Синтаксис**

``` sql
OPTIMIZE TABLE table DEDUPLICATE; -- по всем столбцам
OPTIMIZE TABLE table DEDUPLICATE BY *; -- исключаются столбцы MATERIALIZED и ALIAS
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

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
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```
``` sql
INSERT INTO example (primary_key, secondary_key, value, partition_key) 
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```
``` sql
SELECT * FROM example;
```
Результат:
```

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Если в запросе не указаны столбцы, по которым нужно дедуплицировать, то учитываются все столбцы таблицы. Строка удаляется только в том случае, если все значения во всех столбцах равны соответствующим значениям в другой строке.
``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```
``` sql
SELECT * FROM example;
```
Результат:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Если столбцы в запросе указаны через `*`, то дедупликация пройдет по всем столбцам, кроме `ALIAS` и `MATERIALIZED`. Для таблицы `example` будут учтены: `primary_key`, `secondary_key`, `value` и `partition_key`.
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```
``` sql
SELECT * FROM example;
```
Результат:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Дедупликация по всем столбцам, кроме `ALIAS` и `MATERIALIZED` (`BY *`), и с исключением столбца `value`: `primary_key`, `secondary_key` и `partition_key`.
``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```
``` sql
SELECT * FROM example;
```
Результат:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Дедупликация по столбцам `primary_key`, `secondary_key` и `partition_key`.
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```
``` sql
SELECT * FROM example;
```
Результат:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Дедупликация по любому столбцу, который соответствует регулярному выражению `.*_key`: `primary_key`, `secondary_key` и `partition_key`.
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```
``` sql
SELECT * FROM example;
```
Результат:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```