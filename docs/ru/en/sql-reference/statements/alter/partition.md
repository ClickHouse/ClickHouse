---
description: 'Документация по партициям'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: 'Управление партициями и частями'
doc_type: 'reference'
---

Доступны следующие операции с [партициями](/ru/engines/table-engines/mergetree-family/custom-partitioning-key.md):

* [DETACH PARTITION|PART](#detach-partitionpart) — Перемещает партицию или часть в каталог `detached` и исключает её из использования.
* [DROP PARTITION|PART](#drop-partitionpart) — Удаляет партицию или часть.
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - Удаляет часть или все части партиции из `detached`.
* [FORGET PARTITION](#forget-partition) — Удаляет метаданные партиции из ZooKeeper, если она пуста.
* [ATTACH PARTITION|PART](#attach-partitionpart) — Добавляет партицию или часть из каталога `detached` в таблицу.
* [ATTACH PARTITION FROM](#attach-partition-from) — Копирует партицию данных из одной таблицы в другую и добавляет её.
* [REPLACE PARTITION](#replace-partition) — Копирует партицию данных из одной таблицы в другую и заменяет ею существующую.
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — Перемещает партицию данных из одной таблицы в другую.
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — Сбрасывает значение указанного столбца в партиции.
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — Сбрасывает указанный вторичный индекс в партиции.
* [FREEZE PARTITION](#freeze-partition) — Создаёт резервную копию партиции.
* [UNFREEZE PARTITION](#unfreeze-partition) — Удаляет резервную копию партиции.
* [FETCH PARTITION|PART](#fetch-partitionpart) — Загружает часть или партицию с другого сервера.
* [MOVE PARTITION|PART](#move-partitionpart) — Перемещает партицию/часть данных на другой диск или том.
* [UPDATE IN PARTITION](#update-in-partition) — Обновляет данные внутри партиции по условию.
* [DELETE IN PARTITION](#delete-in-partition) — Удаляет данные внутри партиции по условию.
* [REWRITE PARTS](#rewrite-parts) — Полностью переписывает части в таблице (или в конкретной партиции).

{/* */ }

<div id="detach-partitionpart">
  ## DETACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

Перемещает все данные указанной партиции в каталог `detached`. Сервер перестаёт учитывать отсоединённую партицию данных, как будто её не существует. Сервер не будет знать об этих данных, пока вы не выполните запрос [ATTACH](#attach-partitionpart).

Пример:

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

Подробнее о задании выражения партиционирования см. в разделе [Как задать выражение партиционирования](#how-to-set-partition-expression).

После выполнения запроса вы можете делать с данными в каталоге `detached` всё, что угодно: удалить их из файловой системы или просто оставить.

Этот запрос реплицируется — данные будут перемещены в каталог `detached` на всех репликах. Обратите внимание: выполнить этот запрос можно только на реплике-лидере. Чтобы узнать, является ли реплика лидером, выполните запрос `SELECT` к таблице [system.replicas](/ru/operations/system-tables/replicas). Либо, что проще, выполните запрос `DETACH` на всех репликах: все реплики сгенерируют исключение, кроме реплик-лидеров (так как может быть несколько лидеров).

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

Удаляет указанную партицию из таблицы. Этот запрос помечает партицию как неактивную и полностью удаляет данные примерно через 10 минут.

О том, как задать выражение партиционирования, читайте в разделе [Как задать выражение партиционирования](#how-to-set-partition-expression).

Запрос реплицируется — данные удаляются на всех репликах.

Пример:

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

Удаляет указанную часть или все части указанной партиции из `detached`.
Подробнее о том, как задать выражение партиционирования, см. в разделе [Как задать выражение партиционирования](#how-to-set-partition-expression).

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

Удаляет все метаданные о пустой партиции из ZooKeeper. Запрос завершится ошибкой, если партиция не пуста или не существует. Выполняйте этот запрос только для партиций, которые больше никогда не будут использоваться.

О том, как задать выражение партиционирования, читайте в разделе [Как задать выражение партиционирования](#how-to-set-partition-expression).

Пример:

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Добавляет данные в таблицу из каталога `detached`. Можно добавить данные как для всей партиции, так и для отдельной части. Примеры:

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Подробнее о задании выражения партиционирования читайте в разделе [Как задать выражение партиционирования](#how-to-set-partition-expression).

Этот запрос реплицируется. Реплика-инициатор проверяет, есть ли данные в каталоге `detached`.
Если данные есть, запрос проверяет их целостность. Если всё в порядке, запрос добавляет данные в таблицу.

Если реплика, не являющаяся инициатором и получившая команду attach, находит часть с корректными контрольными суммами в собственном каталоге `detached`, она подключает данные без получения их с других реплик.
Если части с корректными контрольными суммами нет, данные загружаются с любой реплики, у которой есть эта часть.

Вы можете поместить данные в каталог `detached` на одной реплике и использовать запрос `ALTER ... ATTACH`, чтобы добавить их в таблицу на всех репликах.

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

Этот запрос копирует партицию данных из `table1` в `table2`.

Обратите внимание:

* Данные не будут удалены ни из `table1`, ни из `table2`.
* `table1` может быть временной таблицей.

Чтобы запрос выполнился успешно, должны быть соблюдены следующие условия:

* Обе таблицы должны иметь одинаковую структуру.
* Обе таблицы должны иметь одинаковый ключ партиционирования, одинаковый ключ ORDER BY и одинаковый первичный ключ.
* Обе таблицы должны иметь одинаковую политику хранения.
* Целевая таблица должна содержать все индексы и проекции исходной таблицы. Если в целевой таблице включен параметр `enforce_index_structure_match_on_partition_manipulation`, индексы и проекции должны быть идентичны. В противном случае целевая таблица может содержать надмножество индексов и проекций исходной таблицы.

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

Этот запрос копирует партицию данных из `table1` в `table2` и заменяет существующую партицию в `table2`. Операция выполняется атомарно.

Обратите внимание:

* Данные из `table1` не удаляются.
* `table1` может быть временной таблицей.

Чтобы запрос выполнился успешно, должны быть соблюдены следующие условия:

* Обе таблицы должны иметь одинаковую структуру.
* Обе таблицы должны иметь одинаковый ключ партиционирования, одинаковый ключ ORDER BY и одинаковый первичный ключ.
* Обе таблицы должны иметь одинаковую политику хранения.
* Целевая таблица должна включать все индексы и проекции из исходной таблицы. Если в целевой таблице включен параметр `enforce_index_structure_match_on_partition_manipulation`, индексы и проекции должны быть идентичны. В противном случае целевая таблица может содержать надмножество индексов и проекций исходной таблицы.

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

Этот запрос перемещает партицию данных из `table_source` в `table_dest` с удалением данных из `table_source`.

Чтобы запрос успешно выполнился, должны быть соблюдены следующие условия:

* Обе таблицы должны иметь одинаковую структуру.
* Обе таблицы должны иметь одинаковый ключ партиционирования, одинаковый ключ ORDER BY и одинаковый первичный ключ.
* Обе таблицы должны иметь одинаковую политику хранения.
* Обе таблицы должны относиться к одному и тому же семейству движков (реплицируемых или нереплицируемых).
* Целевая таблица должна содержать все индексы и проекции из исходной таблицы. Если в целевой таблице включен параметр `enforce_index_structure_match_on_partition_manipulation`, индексы и проекции должны быть идентичными. В противном случае целевая таблица может содержать надмножество индексов и проекций исходной таблицы.

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

Сбрасывает все значения в указанном столбце партиции. Если при создании таблицы была задана клауза `DEFAULT`, этот запрос устанавливает для столбца указанное значение по умолчанию.

Пример:

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

Этот запрос создает локальную резервную копию указанной партиции. Если предложение `PARTITION` опущено, запрос создает резервную копию всех партиций сразу.

:::note
Весь процесс резервного копирования выполняется без остановки сервера.
:::

Обратите внимание: для таблиц старого формата можно указать префикс имени партиции (например, `2019`) — тогда запрос создаст резервную копию всех соответствующих партиций. О том, как задать выражение партиционирования, читайте в разделе [Как задать выражение партиционирования](#how-to-set-partition-expression).

Во время выполнения, чтобы создать снимок данных, запрос создает жесткие ссылки на данные таблицы. Жесткие ссылки помещаются в каталог `/var/lib/clickhouse/shadow/N/...`, где:

* `/var/lib/clickhouse/` — рабочий каталог ClickHouse, указанный в конфигурации.
* `N` — порядковый номер резервной копии.
* если указан параметр `WITH NAME`, то вместо порядкового номера используется значение параметра `'backup_name'`.

:::note
Если вы используете [набор дисков для хранения данных таблицы](/ru/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes), каталог `shadow/N` появляется на каждом диске и содержит части данных, соответствующие выражению `PARTITION`.
:::

Внутри резервной копии создается та же структура каталогов, что и в `/var/lib/clickhouse/`. Запрос выполняет `chmod` для всех файлов, запрещая запись в них.

После создания резервной копии вы можете скопировать данные из `/var/lib/clickhouse/shadow/` на удаленный сервер, а затем удалить их с локального сервера. Обратите внимание, что запрос `ALTER t FREEZE PARTITION` не реплицируется. Он создает локальную резервную копию только на локальном сервере.

Запрос создает резервную копию почти мгновенно (но сначала дожидается завершения текущих запросов к соответствующей таблице).

`ALTER TABLE t FREEZE PARTITION` копирует только данные, а не метаданные таблицы. Чтобы создать резервную копию метаданных таблицы, скопируйте файл `/var/lib/clickhouse/metadata/database/table.sql`

Чтобы восстановить данные из резервной копии, выполните следующие действия:

1. Создайте таблицу, если она не существует. Чтобы посмотреть запрос, используйте файл .sql (замените в нем `ATTACH` на `CREATE`).
2. Скопируйте данные из каталога `data/database/table/` внутри резервной копии в каталог `/var/lib/clickhouse/data/database/table/detached/`.
3. Выполните запросы `ALTER TABLE t ATTACH PARTITION`, чтобы добавить данные в таблицу.

Для восстановления из резервной копии останавливать сервер не требуется.

Запрос обрабатывает части параллельно, количество потоков регулируется настройкой `max_threads`.

Дополнительные сведения о резервных копиях и восстановлении данных см. в разделе [&quot;Backup and Restore in ClickHouse&quot;](/ru/operations/backup/overview).

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

Удаляет с диска `frozen`-партиции с указанным именем. Если предложение `PARTITION` опущено, запрос удаляет резервные копии всех партиций сразу.

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

Запрос работает аналогично `CLEAR COLUMN`, но сбрасывает индекс, а не данные столбца.

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

Загружает партицию с другого сервера. Этот запрос работает только с таблицами с репликацией.

Запрос выполняет следующие действия:

1. Загружает партицию|часть с указанного сегмента. В &#39;path-in-zookeeper&#39; необходимо указать путь к сегменту в ZooKeeper.
2. Затем запрос помещает загруженные данные в каталог `detached` таблицы `table_name`. Используйте запрос [ATTACH PARTITION|PART](#attach-partitionpart), чтобы добавить данные в таблицу.

Например:

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

Обратите внимание:

* Запрос `ALTER ... FETCH PARTITION|PART` не реплицируется. Он помещает часть или партицию в каталог `detached` только на локальном сервере.
* Запрос `ALTER TABLE ... ATTACH` реплицируется. Он добавляет данные на все реплики. На одну из реплик данные добавляются из каталога `detached`, а на остальные — с соседних реплик.

Перед скачиванием система проверяет, существует ли партиция и совпадает ли структура таблицы. Наиболее подходящая реплика автоматически выбирается из числа исправных реплик.

Хотя запрос называется `ALTER TABLE`, он не изменяет структуру таблицы и не приводит к немедленному изменению данных, доступных в таблице.

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

Перемещает партиции или части данных на другой том или диск в таблицах с движком `MergeTree`. См. [Использование нескольких блочных устройств для хранения данных](/ru/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes).

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

Запрос `ALTER TABLE t MOVE`:

* Не реплицируется, поскольку у разных реплик могут быть разные политики хранения.
* Возвращает ошибку, если указанный диск или том не настроен. Запрос также возвращает ошибку, если невозможно применить условия перемещения данных, указанные в политике хранения.
* Может возвращать ошибку в случае, когда данные для перемещения уже были перемещены фоновым процессом, параллельным запросом `ALTER TABLE t MOVE` или в результате фонового слияния данных. В этом случае пользователю не нужно выполнять какие-либо дополнительные действия.

Пример:

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

Изменяет данные в указанной партиции в соответствии с указанным выражением фильтрации. Реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

Синтаксис:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Пример
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### См. также
</div>

* [UPDATE](/ru/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

Удаляет данные в указанной партиции, удовлетворяющие указанному выражению фильтрации. Реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

Синтаксис:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Пример
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

Это перепишет части заново, используя все новые настройки. Это оправданно, поскольку настройки на уровне таблицы, такие как `use_const_adaptive_granularity`, по умолчанию применяются только к вновь записанным частям.

<div id="example">
  ### Пример
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also">
  ### См. также
</div>

* [DELETE](/ru/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## Как задать выражение партиционирования
</div>

Вы можете указывать выражение партиционирования в запросах `ALTER ... PARTITION` разными способами:

* Как значение из столбца `partition` таблицы `system.parts`. Например, `ALTER TABLE visits DETACH PARTITION 201901`.
* С помощью ключевого слова `ALL`. Его можно использовать только с DROP/DETACH/ATTACH/ATTACH FROM. Например, `ALTER TABLE visits ATTACH PARTITION ALL`.
* Как кортеж выражений или констант, соответствующий (по типам) кортежу ключей партиционирования таблицы. Если ключ партиционирования состоит из одного элемента, выражение нужно обернуть в функцию `tuple (...)`. Например, `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
* С использованием partition ID. Partition ID — это строковый идентификатор партиции (по возможности человекочитаемый), который используется в качестве имени партиции в файловой системе и в ZooKeeper. Partition ID должен быть указан в секции `PARTITION ID` в одинарных кавычках. Например, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
* В запросах [ALTER ATTACH PART](#attach-partitionpart) и [DROP DETACHED PART](#drop-detached-partitionpart) для указания имени части используйте строковый литерал со значением из столбца `name` таблицы [system.detached&#95;parts](/ru/operations/system-tables/detached_parts). Например, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Использование кавычек при указании партиции зависит от типа выражения партиционирования. Например, для типа `String` его значение нужно указывать в кавычках (`'`). Для типов `Date` и `Int*` кавычки не нужны.

Все приведённые выше правила также верны для запроса [OPTIMIZE](/ru/sql-reference/statements/optimize.md). Если при оптимизации непартиционированной таблицы вам нужно указать единственную партицию, задайте выражение `PARTITION tuple()`. Например:

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` указывает партицию, к данным в которой применяются выражения [UPDATE](/ru/sql-reference/statements/alter/update) или [DELETE](/ru/sql-reference/statements/alter/delete) в запросе `ALTER TABLE`. Новые части создаются только для указанной партиции. Таким образом, `IN PARTITION` помогает снизить нагрузку, когда таблица разделена на множество партиций и требуется обновить данные точечно.

Примеры запросов `ALTER ... PARTITION` приведены в тестах [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) и [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).