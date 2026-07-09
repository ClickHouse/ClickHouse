---
description: 'Документация по изменению столбцов'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: 'Изменение столбцов'
doc_type: 'reference'
---

Набор запросов, позволяющих изменять структуру таблицы.

Синтаксис:

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

В запросе укажите список из одного или нескольких действий, разделённых запятыми.
Каждое действие — это операция со столбцом.

Поддерживаются следующие действия:

* [ADD COLUMN](#add-column) — Добавляет в таблицу новый столбец.
* [DROP COLUMN](#drop-column) — Удаляет столбец.
* [RENAME COLUMN](#rename-column) — Переименовывает существующий столбец.
* [CLEAR COLUMN](#clear-column) — Сбрасывает значения столбца.
* [COMMENT COLUMN](#comment-column) — Добавляет к столбцу текстовый комментарий.
* [MODIFY COLUMN](#modify-column) — Изменяет тип столбца, выражение по умолчанию, TTL и настройки столбца.
* [MODIFY COLUMN REMOVE](#modify-column-remove) — Удаляет одно из свойств столбца.
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) — Изменяет настройки столбца.
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) — Сбрасывает настройки столбца.
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) — Добавляет новые значения в Enum.
* [MATERIALIZE COLUMN](#materialize-column) — Материализует столбец в частях, где он отсутствует.
  Эти действия подробно описаны ниже.

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

Добавляет в таблицу новый столбец с указанными `name`, `type`, [`codec`](../create/table.md/#column_compression_codec) и `default_expr` (см. раздел [Выражения по умолчанию](/ru/sql-reference/statements/create/table#default_values)).

Если указано `IF NOT EXISTS`, запрос не вернёт ошибку, если столбец уже существует. Если указать `AFTER name_after` (имя другого столбца), столбец будет добавлен после него в списке столбцов таблицы. Если вы хотите добавить столбец в начало таблицы, используйте `FIRST`. В противном случае столбец добавляется в конец таблицы. В цепочке действий `name_after` может быть именем столбца, добавленного в одном из предыдущих действий.

Добавление столбца лишь изменяет структуру таблицы, не выполняя никаких действий с данными. После `ALTER` данные не записываются на диск. Если при чтении из таблицы для столбца отсутствуют данные, подставляются значения по умолчанию (путём вычисления выражения по умолчанию, если оно есть, либо с использованием нулей или пустых строк). Столбец появляется на диске после слияния частей данных (см. [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree.md)).

Такой подход позволяет мгновенно выполнить запрос `ALTER`, не увеличивая объём старых данных.

Пример:

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
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

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

Удаляет столбец с именем `name`. Если указано условие `IF EXISTS`, запрос не вернёт ошибку, если столбец не существует.

Удаляет данные из файловой системы. Поскольку при этом удаляются файлы целиком, запрос выполняется почти мгновенно.

:::tip
Нельзя удалить столбец, если на него ссылается [materialized view](/ru/sql-reference/statements/create/view). В противном случае будет возвращена ошибка.
:::

Пример:

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Переименовывает столбец `name` в `new_name`. Если указано условие `IF EXISTS`, запрос не вернёт ошибку, если такого столбца не существует. Поскольку переименование не затрагивает сами данные, запрос выполняется почти мгновенно.

**ПРИМЕЧАНИЕ**: Столбцы, указанные в ключевом выражении таблицы (через `ORDER BY` или `PRIMARY KEY`), переименовать нельзя. Попытка изменить эти столбцы приведёт к `SQL Error [524]`.

Пример:

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Сбрасывает все данные в столбце в указанной партиции. Подробнее о том, как задать имя партиции, см. в разделе [Как задать выражение партиционирования](../alter/partition.md/#how-to-set-partition-expression).

Если указано условие `IF EXISTS`, запрос не вернёт ошибку, если столбец не существует.

Пример:

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Добавляет комментарий к столбцу. Если указано условие `IF EXISTS`, запрос не вернёт ошибку, если столбца не существует.

У каждого столбца может быть только один комментарий. Если у столбца уже есть комментарий, новый комментарий перезапишет предыдущий.

Комментарии хранятся в столбце `comment_expression`, который возвращается запросом [DESCRIBE TABLE](/ru/sql-reference/statements/describe-table.md).

Пример:

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

Этот запрос изменяет свойства столбца `name`:

* Тип

* Выражение по умолчанию

* Кодек сжатия

* TTL

* Настройки на уровне столбца

* Значения Enum для типов Enum/Enum8/Enum16

Примеры изменения кодеков сжатия столбцов см. в разделе [Кодеки сжатия столбцов](../create/table.md/#column_compression_codec).

Примеры изменения TTL столбцов см. в разделе [TTL столбца](/ru/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl).

Примеры изменения настроек на уровне столбца см. в разделе [Настройки на уровне столбца](/ru/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings).

Если указано условие `IF EXISTS`, запрос не вернёт ошибку, если столбец не существует.

При изменении типа значения преобразуются так, как если бы к ним были применены функции [toType](/ru/sql-reference/functions/type-conversion-functions.md). Если изменяется только выражение по умолчанию, запрос не выполняет никаких сложных операций и завершается почти мгновенно.

Пример:

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Изменение типа столбца — единственное сложное действие: при этом изменяется содержимое файлов с данными. Для больших таблиц это может занять много времени.

Запрос также может изменить порядок столбцов с помощью условия `FIRST | AFTER`, см. описание [ADD COLUMN](#add-column), но в этом случае указание типа столбца обязательно.

Пример:

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

Запрос `ALTER` является атомарным. Для таблиц семейства MergeTree он также выполняется без блокировок.

Запрос `ALTER` для изменения столбцов реплицируется. Инструкции сохраняются в ZooKeeper, после чего каждая реплика применяет их. Все запросы `ALTER` выполняются в одном и том же порядке. Запрос ожидает завершения соответствующих действий на других репликах. Однако запрос на изменение столбцов в реплицируемой таблице может быть прерван, а все действия будут выполнены асинхронно.

:::note
Будьте осторожны при изменении столбца типа Nullable на Non-Nullable. Убедитесь, что в нём нет значений NULL, иначе это вызовет проблемы при чтении. В таком случае в качестве обходного решения можно завершить мутацию и вернуть столбцу тип Nullable.
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

Удаляет одно из свойств столбца: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`, `SETTINGS`.

Синтаксис:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**Пример**

Удалите TTL:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**См. также**

* [REMOVE TTL](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

Изменить настройку столбца.

Синтаксис:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**Пример**

Измените значение `max_compress_block_size` для столбца на `1MB`:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

Сбрасывает настройку столбца, а также удаляет объявление этой настройки из выражения столбца в запросе CREATE таблицы.

Синтаксис:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**Пример**

Сбросьте настройку столбца `max_compress_block_size` до значения по умолчанию:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

Добавляет новые значения в столбец типа `Enum`, `Enum8`, `Enum16`, `Nullable(Enum)`, `Nullable(Enum8)` или `Nullable(Enum16)`.

Синтаксис:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**Пример**

Добавьте два значения в столбец `enum_column_name`:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

Материализует столбец с выражением значения `DEFAULT` или `MATERIALIZED`. При добавлении материализованного столбца с помощью `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED` существующие строки без материализованных значений автоматически не заполняются. Оператор `MATERIALIZE COLUMN` можно использовать, чтобы переписать существующие данные столбца после добавления или обновления выражения `DEFAULT` или `MATERIALIZED` (при этом обновляются только метаданные, а существующие данные не изменяются). Обратите внимание: материализация столбца, входящего в ключ сортировки, недопустима, поскольку может нарушить порядок сортировки.
Реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

Для столбцов с новым или обновлённым выражением значения `MATERIALIZED` переписываются все существующие строки.

Для столбцов с новым или обновлённым выражением значения `DEFAULT` поведение зависит от версии ClickHouse:

* В ClickHouse &lt; v24.2 переписываются все существующие строки.
* В ClickHouse &gt;= v24.2 учитывается, было ли значение в строке столбца с выражением значения `DEFAULT` явно указано при вставке или нет, то есть было ли оно вычислено на основе выражения значения `DEFAULT`. Если значение было указано явно, ClickHouse оставляет его без изменений. Если значение было вычислено, ClickHouse заменяет его новым или обновлённым выражением значения `MATERIALIZED`.

Синтаксис:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* Если вы укажете PARTITION, столбец будет материализован только для указанной партиции.

**Пример**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**См. также**

* [MATERIALIZED](/ru/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## Ограничения
</div>

Запрос `ALTER` позволяет создавать и удалять отдельные элементы (столбцы) во вложенных структурах данных, но не целые вложенные структуры данных. Чтобы добавить вложенную структуру данных, можно добавить столбцы с именем вида `name.nested_name` и типом `Array(T)`. Вложенная структура данных эквивалентна нескольким столбцам типа Array с одинаковым префиксом в имени до точки.

Переименование столбцов с точками в именах поддерживается лишь частично. Точки зарезервированы для доступа к подстолбцам [Nested](/ru/sql-reference/data-types/nested-data-structures/nested), поэтому префикс (имя родителя) должен оставаться неизменным. Можно изменить только суффикс (имя подстолбца). Например, `a.b` можно переименовать в `a.c`, но переименование `a.b` в `b.d` недопустимо, поскольку при этом меняется родительский префикс Nested.

Удаление столбцов, входящих в первичный ключ или ключ выборки (то есть столбцов, используемых в выражении `ENGINE`), не поддерживается. Изменение типа столбцов, входящих в первичный ключ, возможно только в том случае, если оно не приводит к изменению данных (например, можно добавлять значения в Enum или изменять тип с `DateTime` на `UInt32`).

Если запроса `ALTER` недостаточно для внесения необходимых изменений в таблицу, можно создать новую таблицу, скопировать в нее данные с помощью запроса [INSERT SELECT](/ru/sql-reference/statements/insert-into.md/#inserting-the-results-of-select), затем поменять таблицы местами с помощью запроса [RENAME](/ru/sql-reference/statements/rename.md/#rename-table) и удалить старую таблицу.

Запрос `ALTER` блокирует для таблицы все операции чтения и записи. Иными словами, если в момент выполнения запроса `ALTER` выполняется длительный `SELECT`, запрос `ALTER` будет ждать его завершения. В то же время все новые запросы к этой же таблице будут ждать, пока выполняется `ALTER`.

Для таблиц, которые сами не хранят данные (например, [Merge](/ru/sql-reference/statements/alter/index.md) и [Distributed](/ru/sql-reference/statements/alter/index.md)), `ALTER` лишь изменяет структуру таблицы и не изменяет структуру подчиненных таблиц. Например, при выполнении `ALTER` для таблицы `Distributed` вам также потребуется выполнить `ALTER` для таблиц на всех удаленных серверах.