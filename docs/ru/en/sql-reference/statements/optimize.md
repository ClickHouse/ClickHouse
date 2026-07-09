---
description: 'Документация по Optimize'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'Оператор OPTIMIZE'
doc_type: 'reference'
---

Этот запрос пытается инициировать внеплановое слияние частей данных в таблицах. Обратите внимание, что мы, как правило, не рекомендуем использовать `OPTIMIZE TABLE ... FINAL` (см. [эту документацию](/ru/optimize/avoidoptimizefinal)), поскольку этот сценарий использования предназначен для администрирования, а не для повседневной эксплуатации.

:::note
`OPTIMIZE` не может устранить ошибку `Too many parts`.
:::

**Синтаксис**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

Запрос `OPTIMIZE` поддерживается для семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) (включая [materialized views](/ru/sql-reference/statements/create/view#materialized-view)) и движков [Buffer](../../engines/table-engines/special/buffer.md). Движки таблиц других типов не поддерживаются.

Когда `OPTIMIZE` используется с семейством движков таблиц [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md), ClickHouse создаёт задачу на слияние и ждёт её выполнения на всех репликах (если настройка [alter&#95;sync](/ru/operations/settings/settings#alter_sync) установлена в `2`) или на текущей реплике (если настройка [alter&#95;sync](/ru/operations/settings/settings#alter_sync) установлена в `1`).

* Если `OPTIMIZE` по какой-либо причине не выполняет слияние, клиент не получает уведомление. Чтобы включить уведомления, используйте настройку [optimize&#95;throw&#95;if&#95;noop](/ru/operations/settings/settings#optimize_throw_if_noop).
* Если вы укажете `PARTITION`, будет оптимизирована только указанная партиция. [Как задать выражение партиционирования](alter/partition.md#how-to-set-partition-expression).
* Если вы укажете `FINAL` или `FORCE`, оптимизация будет выполнена, даже если все данные уже находятся в одной части. Этим поведением можно управлять с помощью [optimize&#95;skip&#95;merged&#95;partitions](/ru/operations/settings/settings#optimize_skip_merged_partitions). Кроме того, слияние будет принудительно выполнено, даже если одновременно выполняются другие слияния.
* Если вы укажете `DEDUPLICATE`, полностью идентичные строки (если не указано условие BY) будут дедуплицированы (сравниваются все столбцы); это имеет смысл только для движка MergeTree.

С помощью настройки [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ru/operations/settings/settings#replication_wait_for_inactive_replica_timeout) можно указать, как долго (в секундах) ждать выполнения запросов `OPTIMIZE` неактивными репликами.

:::note
Если `alter_sync` установлен в `2` и некоторые реплики остаются неактивными дольше времени, заданного в настройке `replication_wait_for_inactive_replica_timeout`, будет сгенерировано исключение `UNFINISHED`.
:::

<div id="dry-run">
  ## DRY RUN
</div>

Клауза `DRY RUN` имитирует слияние указанных частей без коммита результата. Слитая часть записывается во временное место, проверяется, а затем удаляется. Исходные части и данные таблицы остаются без изменений.

Это полезно для:

* Проверки корректности слияния в разных версиях ClickHouse.
* Детерминированного воспроизведения ошибок, связанных со слиянием.
* Бенчмаркинга производительности слияния.

`DRY RUN` поддерживается только для таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Требуется ключевое слово `PARTS` со списком имен частей. Все указанные части должны существовать, быть активными и принадлежать одной партиции.

`DRY RUN` несовместим с `FINAL` и `PARTITION`. Его можно комбинировать с `DEDUPLICATE` (с необязательным указанием столбцов) и `CLEANUP` (для таблиц `ReplacingMergeTree`).

**Синтаксис**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

По умолчанию результирующая слитая часть проверяется аналогично запросу [`CHECK TABLE`](/ru/sql-reference/statements/check-table). Это поведение управляется настройкой [optimize&#95;dry&#95;run&#95;check&#95;part](/ru/operations/settings/settings#optimize_dry_run_check_part) (по умолчанию включена). Если отключить эту настройку, проверка будет пропущена, что может быть полезно для бенчмаркинга самого слияния.

**Пример**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## Выражение BY
</div>

Если вы хотите выполнять дедупликацию по произвольному набору столбцов, а не по всем столбцам, можно явно указать список столбцов или использовать любую комбинацию выражений [`*`](../../sql-reference/statements/select/index.md#asterisk), [`COLUMNS`](/ru/sql-reference/statements/select#select-clause) и [`EXCEPT`](/ru/sql-reference/statements/select/except-modifier). Явно заданный или неявно развернутый список столбцов должен включать все столбцы, указанные в выражении сортировки строк (и основной ключ, и ключ сортировки), а также в выражении партиционирования (ключ партиционирования).

:::note
Обратите внимание, что `*` ведёт себя так же, как в `SELECT`: столбцы [MATERIALIZED](/ru/sql-reference/statements/create/view#materialized-view) и [ALIAS](../../sql-reference/statements/create/table.md#alias) при развертывании не используются.

Также ошибкой считается указание пустого списка столбцов, запись выражения, которое приводит к пустому списку столбцов, или выполнение дедупликации по столбцу `ALIAS`.
:::

**Синтаксис**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**Примеры**

Рассмотрим таблицу:

```sql title="Query"
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

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

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

Все приведённые ниже примеры выполняются для этого состояния из 5 строк.

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

Если столбцы для дедупликации не указаны, учитываются все столбцы. Строка удаляется только в том случае, если все значения во всех столбцах совпадают с соответствующими значениями в предыдущей строке:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

Если столбцы указаны неявно, дедупликация таблицы выполняется по всем столбцам, которые не являются `ALIAS` или `MATERIALIZED`. Для приведённой выше таблицы это столбцы `primary_key`, `secondary_key`, `value` и `partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

Выполняйте дедупликацию по всем столбцам, кроме `ALIAS` и `MATERIALIZED`, а также явно исключая `value`: столбцы `primary_key`, `secondary_key` и `partition_key`.

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

Явно выполните дедупликацию по столбцам `primary_key`, `secondary_key` и `partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

Выполнять дедупликацию по всем столбцам, имена которых соответствуют регулярному выражению: `primary_key`, `secondary_key` и `partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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