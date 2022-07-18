---
sidebar_position: 38
sidebar_label: PARTITION
---

# Манипуляции с партициями и кусками {#alter_manipulations-with-partitions}

Для работы с [партициями](../../../engines/table-engines/mergetree-family/custom-partitioning-key.md) доступны следующие операции:

-   [DETACH PARTITION](#alter_detach-partition) — перенести партицию в директорию `detached`;
-   [DROP PARTITION](#alter_drop-partition) — удалить партицию;
-   [ATTACH PARTITION\|PART](#alter_attach-partition) — добавить партицию/кусок в таблицу из директории `detached`;
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) — скопировать партицию из другой таблицы;
-   [REPLACE PARTITION](#alter_replace-partition) — скопировать партицию из другой таблицы с заменой;
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition) — переместить партицию в другую таблицу;
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) — удалить все значения в столбце для заданной партиции;
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) — очистить построенные вторичные индексы для заданной партиции;
-   [FREEZE PARTITION](#alter_freeze-partition) — создать резервную копию партиции;
-   [UNFREEZE PARTITION](#alter_unfreeze-partition) — удалить резервную копию партиции;
-   [FETCH PARTITION\|PART](#alter_fetch-partition) — скачать партицию/кусок с другого сервера;
-   [MOVE PARTITION\|PART](#alter_move-partition) — переместить партицию/кускок на другой диск или том.
-   [UPDATE IN PARTITION](#update-in-partition) — обновить данные внутри партиции по условию.
-   [DELETE IN PARTITION](#delete-in-partition) — удалить данные внутри партиции по условию.

## DETACH PARTITION\|PART {#alter_detach-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

Перемещает заданную партицию в директорию `detached`. Сервер не будет знать об этой партиции до тех пор, пока вы не выполните запрос [ATTACH](#alter_attach-partition).

Пример:

``` sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

После того как запрос будет выполнен, вы сможете производить любые операции с данными в директории `detached`. Например, можно удалить их из файловой системы.

Запрос реплицируется — данные будут перенесены в директорию `detached` и забыты на всех репликах. Обратите внимание, запрос может быть отправлен только на реплику-лидер. Чтобы узнать, является ли реплика лидером, выполните запрос `SELECT` к системной таблице [system.replicas](../../../operations/system-tables/replicas.md#system_tables-replicas). Либо можно выполнить запрос `DETACH` на всех репликах — тогда на всех репликах, кроме реплик-лидеров (поскольку допускается несколько лидеров), запрос вернет ошибку.

## DROP PARTITION\|PART {#alter_drop-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

Удаляет партицию. Партиция помечается как неактивная и будет полностью удалена примерно через 10 минут.

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Запрос реплицируется — данные будут удалены на всех репликах.

Пример:

``` sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

## DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART partition_expr
```

Удаляет из `detached` кусок или все куски, принадлежащие партиции.
Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

## ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] ATTACH PARTITION|PART partition_expr
```

Добавляет данные в таблицу из директории `detached`. Можно добавить данные как для целой партиции, так и для отдельного куска. Примеры:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Как корректно задать имя партиции или куска, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Этот запрос реплицируется. Реплика-иницатор проверяет, есть ли данные в директории `detached`.
Если данные есть, то запрос проверяет их целостность. В случае успеха данные добавляются в таблицу.

Если реплика, не являющаяся инициатором запроса, получив команду присоединения, находит кусок с правильными контрольными суммами в своей собственной папке `detached`, она присоединяет данные, не скачивая их с других реплик.
Если нет куска с правильными контрольными суммами, данные загружаются из любой реплики, имеющей этот кусок.

Вы можете поместить данные в директорию `detached` на одной реплике и с помощью запроса `ALTER ... ATTACH` добавить их в таблицу на всех репликах.

## ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

Копирует партицию из таблицы `table1` в таблицу `table2`.
Обратите внимание, что данные не удаляются ни из `table1`, ни из `table2`.

Следует иметь в виду:

-   Таблицы должны иметь одинаковую структуру.
-   Для таблиц должна быть задана одинаковая политика хранения (диск, на котором хранится партиция, должен быть доступен для обеих таблиц).
-   Для таблиц должен быть задан одинаковый ключ партиционирования, одинаковый ключ сортировки и одинаковый первичный ключ.

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

## REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

Копирует партицию из таблицы `table1` в таблицу `table2` с заменой существующих данных в `table2`. Данные из `table1` не удаляются.

Следует иметь в виду:

-   Таблицы должны иметь одинаковую структуру.
-   Для таблиц должна быть задана одинаковая политика хранения (диск, на котором хранится партиция, должен быть доступен для обеих таблиц).
-   Для таблиц должен быть задан одинаковый ключ партиционирования, одинаковый ключ сортировки и одинаковый первичный ключ.

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

## MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

Перемещает партицию из таблицы `table_source` в таблицу `table_dest` (добавляет к существующим данным в `table_dest`) с удалением данных из таблицы `table_source`.

Следует иметь в виду:

-   Таблицы должны иметь одинаковую структуру.
-   Для таблиц должен быть задан одинаковый ключ партиционирования, одинаковый ключ сортировки и одинаковый первичный ключ.
-   Для таблиц должна быть задана одинаковая политика хранения (диск, на котором хранится партиция, должен быть доступен для обеих таблиц).
-   Движки таблиц должны быть одинакового семейства (реплицированные или нереплицированные).

## CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

Сбрасывает все значения в столбце для заданной партиции. Если для столбца определено значение по умолчанию (в секции `DEFAULT`), то будет выставлено это значение.

Пример:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

## CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

Работает как `CLEAR COLUMN`, но сбрасывает индексы вместо данных в столбцах.

## FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

Создаёт резервную копию для заданной партиции. Если выражение `PARTITION` опущено, резервные копии будут созданы для всех партиций.

    :::note "Примечание"
    Создание резервной копии не требует остановки сервера.
    :::
Для таблиц старого стиля имя партиций можно задавать в виде префикса (например, `2019`). В этом случае, резервные копии будут созданы для всех соответствующих партиций. Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Запрос формирует для текущего состояния таблицы жесткие ссылки на данные в этой таблице. Ссылки размещаются в директории `/var/lib/clickhouse/shadow/N/...`, где:

-   `/var/lib/clickhouse/` — рабочая директория ClickHouse, заданная в конфигурационном файле;
-   `N` — инкрементальный номер резервной копии.
-   если задан параметр `WITH NAME`, то вместо инкрементального номера используется значение параметра `'backup_name'`.

    :::note "Примечание"
    При использовании [нескольких дисков для хранения данных таблицы](../../statements/alter/index.md#table_engine-mergetree-multiple-volumes) директория `shadow/N` появляется на каждом из дисков, на которых были куски, попавшие под выражение `PARTITION`.
    :::
Структура директорий внутри резервной копии такая же, как внутри `/var/lib/clickhouse/`. Запрос выполнит `chmod` для всех файлов, запрещая запись в них.

Обратите внимание, запрос `ALTER TABLE t FREEZE PARTITION` не реплицируется. Он создает резервную копию только на локальном сервере. После создания резервной копии данные из `/var/lib/clickhouse/shadow/` можно скопировать на удалённый сервер, а локальную копию удалить.

Резервная копия создается почти мгновенно (однако, сначала запрос дожидается завершения всех запросов, которые выполняются для соответствующей таблицы).

`ALTER TABLE t FREEZE PARTITION` копирует только данные, но не метаданные таблицы. Чтобы сделать резервную копию метаданных таблицы, скопируйте файл `/var/lib/clickhouse/metadata/database/table.sql`

Чтобы восстановить данные из резервной копии, выполните следующее:

1.  Создайте таблицу, если она ещё не существует. Запрос на создание можно взять из .sql файла (замените в нём `ATTACH` на `CREATE`).
2.  Скопируйте данные из директории `data/database/table/` внутри резервной копии в директорию `/var/lib/clickhouse/data/database/table/detached/`.
3.  С помощью запросов `ALTER TABLE t ATTACH PARTITION` добавьте данные в таблицу.

Восстановление данных из резервной копии не требует остановки сервера.

Подробнее о резервном копировании и восстановлении данных читайте в разделе [Резервное копирование данных](../../../operations/backup.md).

## UNFREEZE PARTITION {#alter_unfreeze-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

Удаляет с диска "замороженные" партиции с указанным именем. Если секция `PARTITION` опущена, запрос удаляет резервную копию всех партиций сразу.

## FETCH PARTITION\|PART {#alter_fetch-partition}

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

Загружает партицию с другого сервера. Этот запрос работает только для реплицированных таблиц.

Запрос выполняет следующее:

1.  Загружает партицию/кусок с указанного шарда. Путь к шарду задается в секции `FROM` (‘path-in-zookeeper’). Обратите внимание, нужно задавать путь к шарду в ZooKeeper.
2.  Помещает загруженные данные в директорию `detached` таблицы `table_name`. Чтобы прикрепить эти данные к таблице, используйте запрос [ATTACH PARTITION\|PART](#alter_attach-partition).

Например:

1. FETCH PARTITION
``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```
2. FETCH PART
``` sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

Следует иметь в виду:

-   Запрос `ALTER TABLE t FETCH PARTITION|PART` не реплицируется. Он загружает партицию в директорию `detached` только на локальном сервере.
-   Запрос `ALTER TABLE t ATTACH` реплицируется — он добавляет данные в таблицу сразу на всех репликах. На одной из реплик данные будут добавлены из директории `detached`, а на других — из соседних реплик.

Перед загрузкой данных система проверяет, существует ли партиция и совпадает ли её структура со структурой таблицы. При этом автоматически выбирается наиболее актуальная реплика среди всех живых реплик.

Несмотря на то что запрос называется `ALTER TABLE`, он не изменяет структуру таблицы и не изменяет сразу доступные данные в таблице.

## MOVE PARTITION\|PART {#alter_move-partition}

Перемещает партицию или кусок данных на другой том или диск для таблиц с движком `MergeTree`. Смотрите [Хранение данных таблицы на нескольких блочных устройствах](../../statements/alter/index.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

Запрос `ALTER TABLE t MOVE`:

-   Не реплицируется, т.к. на разных репликах могут быть различные конфигурации политик хранения.
-   Возвращает ошибку, если указан несконфигурированный том или диск. Ошибка также возвращается в случае невыполнения условий перемещения данных, которые указаны в конфигурации политики хранения.
-   Может возвращать ошибку в случае, когда перемещаемые данные уже оказались перемещены в результате фонового процесса, конкурентного запроса `ALTER TABLE t MOVE` или как часть результата фоновой операции слияния. В данном случае никаких дополнительных действий от пользователя не требуется.

Примеры:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

## UPDATE IN PARTITION {#update-in-partition}

Манипулирует данными в указанной партиции, соответствующими заданному выражению фильтрации. Реализовано как мутация [mutation](../../../sql-reference/statements/alter/index.md#mutations).

Синтаксис:

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

### Пример

``` sql
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;
```

### Смотрите также

-   [UPDATE](../../../sql-reference/statements/alter/update.md#alter-table-update-statements)

## DELETE IN PARTITION {#delete-in-partition}

Удаляет данные в указанной партиции, соответствующие указанному выражению фильтрации. Реализовано как мутация [mutation](../../../sql-reference/statements/alter/index.md#mutations).

Синтаксис:

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_id] WHERE filter_expr
```

### Пример

``` sql
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;
```

### Смотрите также

-   [DELETE](../../../sql-reference/statements/alter/delete.md#alter-mutations)

## Как задавать имя партиции в запросах ALTER {#alter-how-to-specify-part-expr}

Чтобы задать нужную партицию в запросах `ALTER ... PARTITION`, можно использовать:

-   Имя партиции. Посмотреть имя партиции можно в столбце `partition` системной таблицы [system.parts](../../../operations/system-tables/parts.md#system_tables-parts). Например, `ALTER TABLE visits DETACH PARTITION 201901`.
-   Кортеж из выражений или констант, совпадающий (в типах) с кортежем партиционирования. В случае ключа партиционирования из одного элемента, выражение следует обернуть в функцию `tuple(...)`. Например, `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
-   Строковый идентификатор партиции. Идентификатор партиции используется для именования кусков партиции на файловой системе и в ZooKeeper. В запросах `ALTER` идентификатор партиции нужно указывать в секции `PARTITION ID`, в одинарных кавычках. Например, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   Для запросов [ATTACH PART](#alter_attach-partition) и [DROP DETACHED PART](#alter_drop-detached): чтобы задать имя куска партиции, используйте строковой литерал со значением из столбца `name` системной таблицы [system.detached_parts](../../../operations/system-tables/detached_parts.md#system_tables-detached_parts). Например, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Использование кавычек в имени партиций зависит от типа данных столбца, по которому задано партиционирование. Например, для столбца с типом `String` имя партиции необходимо указывать в кавычках (одинарных). Для типов `Date` и `Int*` кавычки указывать не нужно.

Замечание: для таблиц старого стиля партицию можно указывать и как число `201901`, и как строку `'201901'`. Синтаксис для таблиц нового типа более строг к типам (аналогично парсеру входного формата VALUES).

Правила, сформулированные выше, актуальны также для запросов [OPTIMIZE](../../../sql-reference/statements/optimize.md). Чтобы указать единственную партицию непартиционированной таблицы, укажите `PARTITION tuple()`. Например:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` указывает на партицию, для которой применяются выражения [UPDATE](../../../sql-reference/statements/alter/update.md#alter-table-update-statements) или [DELETE](../../../sql-reference/statements/alter/delete.md#alter-mutations) в результате запроса `ALTER TABLE`. Новые куски создаются только в указанной партиции. Таким образом, `IN PARTITION` помогает снизить нагрузку, когда таблица разбита на множество партиций, а вам нужно обновить данные лишь точечно.

Примеры запросов `ALTER ... PARTITION` можно посмотреть в тестах: [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) и [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).
