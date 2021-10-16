---
toc_priority: 37
toc_title: "Манипуляции со столбцами"
---

# Манипуляции со столбцами {#manipuliatsii-so-stolbtsami}

Набор действий, позволяющих изменять структуру таблицы.

Синтаксис:

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|MODIFY|MATERIALIZE COLUMN ...
```

В запросе можно указать сразу несколько действий над одной таблицей через запятую.
Каждое действие — это манипуляция над столбцом.

Существуют следующие действия:

-   [ADD COLUMN](#alter_add-column) — добавляет столбец в таблицу;
-   [DROP COLUMN](#alter_drop-column) — удаляет столбец;
-   [RENAME COLUMN](#alter_rename-column) — переименовывает существующий столбец;
-   [CLEAR COLUMN](#alter_clear-column) — сбрасывает все значения в столбце для заданной партиции;
-   [COMMENT COLUMN](#alter_comment-column) — добавляет комментарий к столбцу;
-   [MODIFY COLUMN](#alter_modify-column) — изменяет тип столбца, выражение для значения по умолчанию и TTL;
-   [MODIFY COLUMN REMOVE](#modify-remove) — удаляет какое-либо из свойств столбца;
-   [MATERIALIZE COLUMN](#materialize-column) — делает столбец материализованным (`MATERIALIZED`) в кусках, в которых отсутствуют значения.

Подробное описание для каждого действия приведено ниже.

## ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

Добавляет в таблицу новый столбец с именем `name`, типом `type`, [кодеком](../create/table.md#codecs) `codec` и выражением для умолчания `default_expr` (смотрите раздел [Значения по умолчанию](../create/index.md#create-default-values)).

Если указано `IF NOT EXISTS`, запрос не будет возвращать ошибку, если столбец уже существует. Если указано `AFTER name_after` (имя другого столбца), то столбец добавляется (в список столбцов таблицы) после указанного. Если вы хотите добавить столбец в начало таблицы, используйте `FIRST`. Иначе столбец добавляется в конец таблицы. Для цепочки действий `name_after` может быть именем столбца, который добавляется в одном из предыдущих действий.

Добавление столбца всего лишь меняет структуру таблицы, и не производит никаких действий с данными - соответствующие данные не появляются на диске после ALTER-а. При чтении из таблицы, если для какого-либо столбца отсутствуют данные, то он заполняется значениями по умолчанию (выполняя выражение по умолчанию, если такое есть, или нулями, пустыми строками). Также, столбец появляется на диске при слиянии кусков данных (см. [MergeTree](../../../sql-reference/statements/alter/index.md)).

Такая схема позволяет добиться мгновенной работы запроса `ALTER` и отсутствия необходимости увеличивать объём старых данных.

Пример:

``` sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

``` text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

## DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Удаляет столбец с именем `name`. Если указано `IF EXISTS`, запрос не будет возвращать ошибку, если столбца не существует.

Запрос удаляет данные из файловой системы. Так как это представляет собой удаление целых файлов, запрос выполняется почти мгновенно.

!!! warning "Предупреждение"
    Вы не можете удалить столбец, используемый в [материализованном представлениии](../../../sql-reference/statements/create/view.md#materialized). В противном случае будет ошибка.

Пример:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

## RENAME COLUMN {#alter_rename-column}

``` sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Переименовывает столбец `name` в `new_name`. Если указано выражение `IF EXISTS`, то запрос не будет возвращать ошибку при условии, что столбец `name` не существует. Поскольку переименование не затрагивает физические данные колонки, запрос выполняется практически мгновенно.

**ЗАМЕЧЕНИЕ**: Столбцы, являющиеся частью основного ключа или ключа сортировки (заданные с помощью `ORDER BY` или `PRIMARY KEY`), не могут быть переименованы. Попытка переименовать эти слобцы приведет к `SQL Error [524]`. 

Пример:

``` sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

## CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Сбрасывает все значения в столбце для заданной партиции. Если указано `IF EXISTS`, запрос не будет возвращать ошибку, если столбца не существует.

Как корректно задать имя партиции, см. в разделе [Как задавать имя партиции в запросах ALTER](#alter-how-to-specify-part-expr).

Пример:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

## COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Добавляет комментарий к таблице. Если указано `IF EXISTS`, запрос не будет возвращать ошибку, если столбца не существует.

Каждый столбец может содержать только один комментарий. При выполнении запроса существующий комментарий заменяется на новый.

Посмотреть комментарии можно в столбце `comment_expression` из запроса [DESCRIBE TABLE](../misc.md#misc-describe-table).

Пример:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'Столбец показывает, из каких браузеров пользователи заходили на сайт.'
```

## MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [codec] [TTL] [AFTER name_after | FIRST]
```

Запрос изменяет следующие свойства столбца `name`:

-   Тип

-   Значение по умолчанию

-   Кодеки сжатия

-   TTL

Примеры изменения кодеков сжатия смотрите в разделе [Кодеки сжатия столбцов](../create/table.md#codecs).

Примеры изменения TTL столбца смотрите в разделе [TTL столбца](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-column-ttl).

Если указано `IF EXISTS`, запрос не возвращает ошибку при условии, что столбец не существует.

Запрос также может изменять порядок столбцов при помощи `FIRST | AFTER`, смотрите описание [ADD COLUMN](#alter_add-column).

При изменении типа, значения преобразуются так, как если бы к ним была применена функция [toType](../../../sql-reference/statements/alter/index.md). Если изменяется только выражение для умолчания, запрос не делает никакой сложной работы и выполняется мгновенно.

Пример запроса:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Изменение типа столбца - это единственное действие, которое выполняет сложную работу - меняет содержимое файлов с данными. Для больших таблиц, выполнение может занять длительное время.

Выполнение запроса ALTER атомарно.

Запрос `ALTER` на изменение столбцов реплицируется. Соответствующие инструкции сохраняются в ZooKeeper, и затем каждая реплика их применяет. Все запросы `ALTER` выполняются в одном и том же порядке. Запрос ждёт выполнения соответствующих действий на всех репликах. Но при этом, запрос на изменение столбцов в реплицируемой таблице можно прервать, и все действия будут осуществлены асинхронно.

## MODIFY COLUMN REMOVE {#modify-remove}

Удаляет какое-либо из свойств столбца: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`.

Синтаксис:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE property;
```

**Пример**

Удаление свойства TTL:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**Смотрите также**

- [REMOVE TTL](ttl.md).

## MATERIALIZE COLUMN {#materialize-column}

Материализует столбец таблицы в кусках, в которых отсутствуют значения. Используется, если необходимо создать новый столбец со сложным материализованным выражением или выражением для заполнения по умолчанию (`DEFAULT`), потому как вычисление такого столбца прямо во время выполнения запроса `SELECT` оказывается ощутимо затратным. Чтобы совершить ту же операцию для существующего столбца, используйте модификатор `FINAL`.

Синтаксис:

```sql
ALTER TABLE table MATERIALIZE COLUMN col [FINAL];
```

**Пример**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 10;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);
SELECT groupArray(x), groupArray(s) FROM tmp;
```

**Результат:**

```sql
┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','5','6','7','8','9'] │
└───────────────────────┴───────────────────────────────────────────┘
```

## Ограничения запроса ALTER {#ogranicheniia-zaprosa-alter}

Запрос `ALTER` позволяет создавать и удалять отдельные элементы (столбцы) вложенных структур данных, но не вложенные структуры данных целиком. Для добавления вложенной структуры данных, вы можете добавить столбцы с именем вида `name.nested_name` и типом `Array(T)` - вложенная структура данных полностью эквивалентна нескольким столбцам-массивам с именем, имеющим одинаковый префикс до точки.

Отсутствует возможность удалять столбцы, входящие в первичный ключ или ключ для сэмплирования (в общем, входящие в выражение `ENGINE`). Изменение типа у столбцов, входящих в первичный ключ возможно только в том случае, если это изменение не приводит к изменению данных (например, разрешено добавление значения в Enum или изменение типа с `DateTime` на `UInt32`).

Если возможностей запроса `ALTER` не хватает для нужного изменения таблицы, вы можете создать новую таблицу, скопировать туда данные с помощью запроса [INSERT SELECT](../insert-into.md#insert_query_insert-select), затем поменять таблицы местами с помощью запроса [RENAME](../misc.md#misc_operations-rename), и удалить старую таблицу. В качестве альтернативы для запроса `INSERT SELECT`, можно использовать инструмент [clickhouse-copier](../../../sql-reference/statements/alter/index.md).

Запрос `ALTER` блокирует все чтения и записи для таблицы. То есть если на момент запроса `ALTER` выполнялся долгий `SELECT`, то запрос `ALTER` сначала дождётся его выполнения. И в это время все новые запросы к той же таблице будут ждать, пока завершится этот `ALTER`.

Для таблиц, которые не хранят данные самостоятельно (типа [Merge](../../../sql-reference/statements/alter/index.md) и [Distributed](../../../sql-reference/statements/alter/index.md)), `ALTER` всего лишь меняет структуру таблицы, но не меняет структуру подчинённых таблиц. Для примера, при ALTER-е таблицы типа `Distributed`, вам также потребуется выполнить запрос `ALTER` для таблиц на всех удалённых серверах.
