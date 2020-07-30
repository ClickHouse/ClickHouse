---
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

Запрос `ALTER` поддерживается только для таблиц типа `*MergeTree`, а также `Merge` и `Distributed`. Запрос имеет несколько вариантов.

### Манипуляции со столбцами {#manipuliatsii-so-stolbtsami}

Изменение структуры таблицы.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

В запросе указывается список из одного или более действий через запятую.
Каждое действие — операция над столбцом.

Существуют следующие действия:

-   [ADD COLUMN](#alter_add-column) — добавляет столбец в таблицу;
-   [DROP COLUMN](#alter_drop-column) — удаляет столбец;
-   [CLEAR COLUMN](#alter_clear-column) — сбрасывает все значения в столбце для заданной партиции;
-   [COMMENT COLUMN](#alter_comment-column) — добавляет комментарий к столбцу;
-   [MODIFY COLUMN](#alter_modify-column) — изменяет тип столбца, выражение для значения по умолчанию и TTL.

Подробное описание для каждого действия приведено ниже.

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

Добавляет в таблицу новый столбец с именем `name`, типом `type`, [кодеком](create.md#codecs) `codec` и выражением для умолчания `default_expr` (смотрите раздел [Значения по умолчанию](create.md#create-default-values)).

Если указано `IF NOT EXISTS`, запрос не будет возвращать ошибку, если столбец уже существует. Если указано `AFTER name_after` (имя другого столбца), то столбец добавляется (в список столбцов таблицы) после указанного. Иначе, столбец добавляется в конец таблицы. Обратите внимание, ClickHouse не позволяет добавлять столбцы в начало таблицы. Для цепочки действий, `name_after` может быть именем столбца, который добавляется в одном из предыдущих действий.

Добавление столбца всего лишь меняет структуру таблицы, и не производит никаких действий с данными - соответствующие данные не появляются на диске после ALTER-а. При чтении из таблицы, если для какого-либо столбца отсутствуют данные, то он заполняется значениями по умолчанию (выполняя выражение по умолчанию, если такое есть, или нулями, пустыми строками). Также, столбец появляется на диске при слиянии кусков данных (см. [MergeTree](../../sql-reference/statements/alter.md)).

Такая схема позволяет добиться мгновенной работы запроса `ALTER` и отсутствия необходимости увеличивать объём старых данных.

Пример:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Удаляет столбец с именем `name`. Если указано `IF EXISTS`, запрос не будет возвращать ошибку, если столбца не существует.

Запрос удаляет данные из файловой системы. Так как это представляет собой удаление целых файлов, запрос выполняется почти мгновенно.

Пример:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Сбрасывает все значения в столбце для заданной партиции. Если указано `IF EXISTS`, запрос не будет возвращать ошибку, если столбца не существует.

Как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Пример:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Добавляет комментарий к таблице. Если указано `IF EXISTS`, запрос не будет возвращать ошибку, если столбца не существует.

Каждый столбец может содержать только один комментарий. При выполнении запроса существующий комментарий заменяется на новый.

Посмотреть комментарии можно в столбце `comment_expression` из запроса [DESCRIBE TABLE](misc.md#misc-describe-table).

Пример:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'Столбец показывает, из каких браузеров пользователи заходили на сайт.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

Запрос изменяет следующие свойства столбца `name`:

-   Тип

-   Значение по умолчанию

-   TTL

        Примеры изменения TTL столбца смотрите в разделе [TTL столбца](../../sql_reference/statements/alter.md#mergetree-column-ttl).

Если указано `IF EXISTS`, запрос не возвращает ошибку, если столбца не существует.

При изменении типа, значения преобразуются так, как если бы к ним была применена функция [toType](../../sql-reference/statements/alter.md). Если изменяется только выражение для умолчания, запрос не делает никакой сложной работы и выполняется мгновенно.

Пример запроса:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Изменение типа столбца - это единственное действие, которое выполняет сложную работу - меняет содержимое файлов с данными. Для больших таблиц, выполнение может занять длительное время.

Выполнение производится в несколько стадий:

-   подготовка временных (новых) файлов с изменёнными данными;
-   переименование старых файлов;
-   переименование временных (новых) файлов в старые;
-   удаление старых файлов.

Из них, длительной является только первая стадия. Если на этой стадии возникнет сбой, то данные не поменяются.
Если на одной из следующих стадий возникнет сбой, то данные будет можно восстановить вручную. За исключением случаев, когда старые файлы удалены из файловой системы, а данные для новых файлов не доехали на диск и потеряны.

Запрос `ALTER` на изменение столбцов реплицируется. Соответствующие инструкции сохраняются в ZooKeeper, и затем каждая реплика их применяет. Все запросы `ALTER` выполняются в одном и том же порядке. Запрос ждёт выполнения соответствующих действий на всех репликах. Но при этом, запрос на изменение столбцов в реплицируемой таблице можно прервать, и все действия будут осуществлены асинхронно.

#### Ограничения запроса ALTER {#ogranicheniia-zaprosa-alter}

Запрос `ALTER` позволяет создавать и удалять отдельные элементы (столбцы) вложенных структур данных, но не вложенные структуры данных целиком. Для добавления вложенной структуры данных, вы можете добавить столбцы с именем вида `name.nested_name` и типом `Array(T)` - вложенная структура данных полностью эквивалентна нескольким столбцам-массивам с именем, имеющим одинаковый префикс до точки.

Отсутствует возможность удалять столбцы, входящие в первичный ключ или ключ для сэмплирования (в общем, входящие в выражение `ENGINE`). Изменение типа у столбцов, входящих в первичный ключ возможно только в том случае, если это изменение не приводит к изменению данных (например, разрешено добавление значения в Enum или изменение типа с `DateTime` на `UInt32`).

Если возможностей запроса `ALTER` не хватает для нужного изменения таблицы, вы можете создать новую таблицу, скопировать туда данные с помощью запроса [INSERT SELECT](insert-into.md#insert_query_insert-select), затем поменять таблицы местами с помощью запроса [RENAME](misc.md#misc_operations-rename), и удалить старую таблицу. В качестве альтернативы для запроса `INSERT SELECT`, можно использовать инструмент [clickhouse-copier](../../sql-reference/statements/alter.md).

Запрос `ALTER` блокирует все чтения и записи для таблицы. То есть, если на момент запроса `ALTER`, выполнялся долгий `SELECT`, то запрос `ALTER` сначала дождётся его выполнения. И в это время, все новые запросы к той же таблице, будут ждать, пока завершится этот `ALTER`.

Для таблиц, которые не хранят данные самостоятельно (типа [Merge](../../sql-reference/statements/alter.md) и [Distributed](../../sql-reference/statements/alter.md)), `ALTER` всего лишь меняет структуру таблицы, но не меняет структуру подчинённых таблиц. Для примера, при ALTER-е таблицы типа `Distributed`, вам также потребуется выполнить запрос `ALTER` для таблиц на всех удалённых серверах.

### Манипуляции с ключевыми выражениями таблиц {#manipuliatsii-s-kliuchevymi-vyrazheniiami-tablits}

Поддерживается операция:

``` sql
MODIFY ORDER BY new_expression
```

Работает только для таблиц семейства [`MergeTree`](../../sql-reference/statements/alter.md) (в том числе [реплицированных](../../sql-reference/statements/alter.md)). После выполнения запроса
[ключ сортировки](../../sql-reference/statements/alter.md) таблицы
заменяется на `new_expression` (выражение или кортеж выражений). Первичный ключ при этом остаётся прежним.

Операция затрагивает только метаданные. Чтобы сохранить свойство упорядоченности кусков данных по ключу
сортировки, разрешено добавлять в ключ только новые столбцы (т.е. столбцы, добавляемые командой `ADD COLUMN`
в том же запросе `ALTER`), у которых нет выражения по умолчанию.

### Манипуляции с индексами {#manipuliatsii-s-indeksami}

Добавить или удалить индекс можно с помощью операций

``` sql
ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value [AFTER name]
ALTER TABLE [db].name DROP INDEX name
```

Поддерживается только таблицами семейства `*MergeTree`.

Команда `ADD INDEX` добавляет описание индексов в метаданные, а `DROP INDEX` удаляет индекс из метаданных и стирает файлы индекса с диска, поэтому они легковесные и работают мгновенно.

Если индекс появился в метаданных, то он начнет считаться в последующих слияниях и записях в таблицу, а не сразу после выполнения операции `ALTER`.

Запрос на изменение индексов реплицируется, сохраняя новые метаданные в ZooKeeper и применяя изменения на всех репликах.

### Манипуляции с ограничениями (constraints) {#manipuliatsii-s-ogranicheniiami-constraints}

Про ограничения подробнее написано [тут](create.md#constraints).

Добавить или удалить ограничение можно с помощью запросов

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

Запросы выполняют добавление или удаление метаданных об ограничениях таблицы `[db].name`, поэтому выполняются мнгновенно.

Если ограничение появилось для непустой таблицы, то *проверка ограничения для имеющихся данных не производится*.

Запрос на изменение ограничений для Replicated таблиц реплицируется, сохраняя новые метаданные в ZooKeeper и применяя изменения на всех репликах.

### Манипуляции с партициями и кусками {#alter_manipulations-with-partitions}

Для работы с [партициями](../../sql-reference/statements/alter.md) доступны следующие операции:

-   [DETACH PARTITION](#alter_detach-partition) — перенести партицию в директорию `detached`;
-   [DROP PARTITION](#alter_drop-partition) — удалить партицию;
-   [ATTACH PARTITION\|PART](#alter_attach-partition) — добавить партицию/кусок в таблицу из директории `detached`;
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) — скопировать партицию из другой таблицы;
-   [REPLACE PARTITION](#alter_replace-partition) — скопировать партицию из другой таблицы с заменой;
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition) — переместить партицию в другую таблицу;
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) — удалить все значения в столбце для заданной партиции;
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) — очистить построенные вторичные индексы для заданной партиции;
-   [FREEZE PARTITION](#alter_freeze-partition) — создать резервную копию партиции;
-   [FETCH PARTITION](#alter_fetch-partition) — скачать партицию с другого сервера;
-   [MOVE PARTITION\|PART](#alter_move-partition) — переместить партицию/кускок на другой диск или том.

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

Перемещает заданную партицию в директорию `detached`. Сервер не будет знать об этой партиции до тех пор, пока вы не выполните запрос [ATTACH](#alter_attach-partition).

Пример:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

После того как запрос будет выполнен, вы сможете производить любые операции с данными в директории `detached`. Например, можно удалить их из файловой системы.

Запрос реплицируется — данные будут перенесены в директорию `detached` и забыты на всех репликах. Обратите внимание, запрос может быть отправлен только на реплику-лидер. Чтобы узнать, является ли реплика лидером, выполните запрос `SELECT` к системной таблице [system.replicas](../../operations/system-tables.md#system_tables-replicas). Либо можно выполнить запрос `DETACH` на всех репликах — тогда на всех репликах, кроме реплики-лидера, запрос вернет ошибку.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Удаляет партицию. Партиция помечается как неактивная и будет полностью удалена примерно через 10 минут.

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Запрос реплицируется — данные будут удалены на всех репликах.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

Удаляет из `detached` кусок или все куски, принадлежащие партиции.
Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Добавляет данные в таблицу из директории `detached`. Можно добавить данные как для целой партиции, так и для отдельного куска. Примеры:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Как корректно задать имя партиции или куска, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Этот запрос реплицируется. Реплика-иницатор проверяет, есть ли данные в директории `detached`. Если данные есть, то запрос проверяет их целостность. В случае успеха данные добавляются в таблицу. Все остальные реплики загружают данные с реплики-инициатора запроса.

Это означает, что вы можете разместить данные в директории `detached` на одной реплике и с помощью запроса `ALTER ... ATTACH` добавить их в таблицу на всех репликах.

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

Копирует партицию из таблицы `table1` в таблицу `table2` и добавляет к существующим данным `table2`. Данные из `table1` не удаляются.

Следует иметь в виду:

-   Таблицы должны иметь одинаковую структуру.
-   Для таблиц должен быть задан одинаковый ключ партиционирования.

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

Копирует партицию из таблицы `table1` в таблицу `table2` с заменой существующих данных в `table2`. Данные из `table1` не удаляются.

Следует иметь в виду:

-   Таблицы должны иметь одинаковую структуру.
-   Для таблиц должен быть задан одинаковый ключ партиционирования.

Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

Перемещает партицию из таблицы `table_source` в таблицу `table_dest` (добавляет к существующим данным в `table_dest`) с удалением данных из таблицы `table_source`.

Следует иметь в виду:

-   Таблицы должны иметь одинаковую структуру.
-   Для таблиц должен быть задан одинаковый ключ партиционирования.
-   Движки таблиц должны быть одинакового семейства (реплицированные или нереплицированные).
-   Для таблиц должна быть задана одинаковая политика хранения.

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

Сбрасывает все значения в столбце для заданной партиции. Если для столбца определено значение по умолчанию (в секции `DEFAULT`), то будет выставлено это значение.

Пример:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

Работает как `CLEAR COLUMN`, но сбрасывает индексы вместо данных в столбцах.

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

Создаёт резервную копию для заданной партиции. Если выражение `PARTITION` опущено, резервные копии будут созданы для всех партиций.

!!! note "Примечание"
    Создание резервной копии не требует остановки сервера.

Для таблиц старого стиля имя партиций можно задавать в виде префикса (например, ‘2019’). В этом случае резервные копии будут созданы для всех соответствующих партиций. Подробнее о том, как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Запрос делает следующее — для текущего состояния таблицы он формирует жесткие ссылки на данные в этой таблице. Ссылки размещаются в директории `/var/lib/clickhouse/shadow/N/...`, где:

-   `/var/lib/clickhouse/` — рабочая директория ClickHouse, заданная в конфигурационном файле;
-   `N` — инкрементальный номер резервной копии.

!!! note "Примечание"
    При использовании [нескольких дисков для хранения данных таблицы](../../sql-reference/statements/alter.md#table_engine-mergetree-multiple-volumes) директория `shadow/N` появляется на каждом из дисков, на которых были куски, попавшие под выражение `PARTITION`.

Структура директорий внутри резервной копии такая же, как внутри `/var/lib/clickhouse/`. Запрос выполнит ‘chmod’ для всех файлов, запрещая запись в них.

Обратите внимание, запрос `ALTER TABLE t FREEZE PARTITION` не реплицируется. Он создает резервную копию только на локальном сервере. После создания резервной копии данные из `/var/lib/clickhouse/shadow/` можно скопировать на удалённый сервер, а локальную копию удалить.

Резервная копия создается почти мгновенно (однако сначала запрос дожидается завершения всех запросов, которые выполняются для соответствующей таблицы).

`ALTER TABLE t FREEZE PARTITION` копирует только данные, но не метаданные таблицы. Чтобы сделать резервную копию метаданных таблицы, скопируйте файл `/var/lib/clickhouse/metadata/database/table.sql`

Чтобы восстановить данные из резервной копии, выполните следующее:

1.  Создайте таблицу, если она ещё не существует. Запрос на создание можно взять из .sql файла (замените в нём `ATTACH` на `CREATE`).
2.  Скопируйте данные из директории `data/database/table/` внутри резервной копии в директорию `/var/lib/clickhouse/data/database/table/detached/`.
3.  С помощью запросов `ALTER TABLE t ATTACH PARTITION` добавьте данные в таблицу.

Восстановление данных из резервной копии не требует остановки сервера.

Подробнее о резервном копировании и восстановлении данных читайте в разделе [Резервное копирование данных](../../operations/backup.md).

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

Загружает партицию с другого сервера. Этот запрос работает только для реплицированных таблиц.

Запрос выполняет следующее:

1.  Загружает партицию с указанного шарда. Путь к шарду задается в секции `FROM` (‘path-in-zookeeper’). Обратите внимание, нужно задавать путь к шарду в ZooKeeper.
2.  Помещает загруженные данные в директорию `detached` таблицы `table_name`. Чтобы прикрепить эти данные к таблице, используйте запрос [ATTACH PARTITION\|PART](#alter_attach-partition).

Например:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

Следует иметь в виду:

-   Запрос `ALTER TABLE t FETCH PARTITION` не реплицируется. Он загружает партицию в директорию `detached` только на локальном сервере.
-   Запрос `ALTER TABLE t ATTACH` реплицируется — он добавляет данные в таблицу сразу на всех репликах. На одной из реплик данные будут добавлены из директории `detached`, а на других — из соседних реплик.

Перед загрузкой данных система проверяет, существует ли партиция и совпадает ли её структура со структурой таблицы. При этом автоматически выбирается наиболее актуальная реплика среди всех живых реплик.

Несмотря на то что запрос называется `ALTER TABLE`, он не изменяет структуру таблицы и не изменяет сразу доступные данные в таблице.

#### MOVE PARTITION\|PART {#alter_move-partition}

Перемещает партицию или кусок данных на другой том или диск для таблиц с движком `MergeTree`. Смотрите [Хранение данных таблицы на нескольких блочных устройствах](../../sql-reference/statements/alter.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
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

#### Как задавать имя партиции в запросах ALTER {#alter-how-to-specify-part-expr}

Чтобы задать нужную партицию в запросах `ALTER ... PARTITION`, можно использовать:

-   Имя партиции. Посмотреть имя партиции можно в столбце `partition` системной таблицы [system.parts](../../operations/system-tables.md#system_tables-parts). Например, `ALTER TABLE visits DETACH PARTITION 201901`.
-   Произвольное выражение из столбцов исходной таблицы. Также поддерживаются константы и константные выражения. Например, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   Строковый идентификатор партиции. Идентификатор партиции используется для именования кусков партиции на файловой системе и в ZooKeeper. В запросах `ALTER` идентификатор партиции нужно указывать в секции `PARTITION ID`, в одинарных кавычках. Например, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   Для запросов [ATTACH PART](#alter_attach-partition) и [DROP DETACHED PART](#alter_drop-detached): чтобы задать имя куска партиции, используйте строковой литерал со значением из столбца `name` системной таблицы [system.detached\_parts](../../operations/system-tables.md#system_tables-detached_parts). Например, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Использование кавычек в имени партиций зависит от типа данных столбца, по которому задано партиционирование. Например, для столбца с типом `String` имя партиции необходимо указывать в кавычках (одинарных). Для типов `Date` и `Int*` кавычки указывать не нужно.

Замечание: для таблиц старого стиля партицию можно указывать и как число `201901`, и как строку `'201901'`. Синтаксис для таблиц нового типа более строг к типам (аналогично парсеру входного формата VALUES).

Правила, сформулированные выше, актуальны также для запросов [OPTIMIZE](misc.md#misc_operations-optimize). Чтобы указать единственную партицию непартиционированной таблицы, укажите `PARTITION tuple()`. Например:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

Примеры запросов `ALTER ... PARTITION` можно посмотреть в тестах: [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) и [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### Манипуляции с TTL таблицы {#manipuliatsii-s-ttl-tablitsy}

Вы можете изменить [TTL для таблицы](../../sql-reference/statements/alter.md#mergetree-table-ttl) запросом следующего вида:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### Мутации {#mutations}

Мутации - разновидность запроса ALTER, позволяющая изменять или удалять данные в таблице. В отличие от стандартных запросов `DELETE` и `UPDATE`, рассчитанных на точечное изменение данных, область применения мутаций - достаточно тяжёлые изменения, затрагивающие много строк в таблице. Поддержана для движков таблиц семейства `MergeTree`, в том числе для движков с репликацией.

Конвертировать существующие таблицы для работы с мутациями не нужно. Но после применения первой мутации формат данных таблицы становится несовместимым с предыдущими версиями и откатиться на предыдущую версию уже не получится.

На данный момент доступны команды:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

Выражение `filter_expr` должно иметь тип `UInt8`. Запрос удаляет строки таблицы, для которых это выражение принимает ненулевое значение.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

Выражение `filter_expr` должно иметь тип `UInt8`. Запрос изменяет значение указанных столбцов на вычисленное значение соответствующих выражений в каждой строке, для которой `filter_expr` принимает ненулевое значение. Вычисленные значения преобразуются к типу столбца с помощью оператора `CAST`. Изменение столбцов, которые используются при вычислении первичного ключа или ключа партиционирования, не поддерживается.

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

Команда перестроит вторичный индекс `name` для партиции `partition_name`.

В одном запросе можно указать несколько команд через запятую.

Для \*MergeTree-таблиц мутации выполняются, перезаписывая данные по кускам (parts). При этом атомарности нет — куски заменяются на помутированные по мере выполнения и запрос `SELECT`, заданный во время выполнения мутации, увидит данные как из измененных кусков, так и из кусков, которые еще не были изменены.

Мутации линейно упорядочены между собой и накладываются на каждый кусок в порядке добавления. Мутации также упорядочены со вставками - гарантируется, что данные, вставленные в таблицу до начала выполнения запроса мутации, будут изменены, а данные, вставленные после окончания запроса мутации, изменены не будут. При этом мутации никак не блокируют вставки.

Запрос завершается немедленно после добавления информации о мутации (для реплицированных таблиц - в ZooKeeper, для нереплицированных - на файловую систему). Сама мутация выполняется асинхронно, используя настройки системного профиля. Следить за ходом её выполнения можно по таблице [`system.mutations`](../../operations/system-tables.md#system_tables-mutations). Добавленные мутации будут выполняться до конца даже в случае перезапуска серверов ClickHouse. Откатить мутацию после её добавления нельзя, но если мутация по какой-то причине не может выполниться до конца, её можно остановить с помощью запроса [`KILL MUTATION`](misc.md#kill-mutation-statement).

Записи о последних выполненных мутациях удаляются не сразу (количество сохраняемых мутаций определяется параметром движка таблиц `finished_mutations_to_keep`). Более старые записи удаляются.

### Синхронность запросов ALTER {#synchronicity-of-alter-queries}

Для нереплицируемых таблиц, все запросы `ALTER` выполняются синхронно. Для реплицируемых таблиц, запрос всего лишь добавляет инструкцию по соответствующим действиям в `ZooKeeper`, а сами действия осуществляются при первой возможности. Но при этом, запрос может ждать завершения выполнения этих действий на всех репликах.

Для запросов `ALTER ... ATTACH|DETACH|DROP` можно настроить ожидание, с помощью настройки `replication_alter_partitions_sync`.
Возможные значения: `0` - не ждать, `1` - ждать выполнения только у себя (по умолчанию), `2` - ждать всех.

Для запросов `ALTER TABLE ... UPDATE|DELETE` синхронность выполнения определяется настройкой [mutations_sync](../../operations/settings/settings.md#mutations_sync). 

## ALTER USER {#alter-user-statement}

Изменяет аккаунт пользователя ClickHouse.

### Синтаксис {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```  

### Описание {#alter-user-dscr}

Для выполнения `ALTER USER` необходима привилегия [ALTER USER](grant.md#grant-access-management).

### Примеры {#alter-user-examples}

Установить ролями по умолчанию роли, назначенные пользователю:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

Если роли не были назначены пользователю, ClickHouse выбрасывает исключение.

Установить ролями по умолчанию все роли, назначенные пользователю:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Если роль будет впоследствии назначена пользователю, она автоматически станет ролью по умолчанию.

Установить ролями по умолчанию все назначенные пользователю роли кроме `role1` и `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```


## ALTER ROLE {#alter-role-statement}

Изменяет роль.

### Синтаксис {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```


## ALTER ROW POLICY {#alter-row-policy-statement}

Изменяет политику доступа к строкам.

### Синтаксис {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```


## ALTER QUOTA {#alter-quota-statement}

Изменяет квоту.

### Синтаксис {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```


## ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

Изменяет профили настроек.

### Синтаксис {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```



[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/) <!--hide-->
