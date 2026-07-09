---
description: 'Документация по оператору INSERT INTO'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'Оператор INSERT INTO'
doc_type: 'reference'
---

Вставляет данные в таблицу.

**Синтаксис**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

Вы можете указать список столбцов для вставки с помощью `(c1, c2, c3)`. Также можно использовать выражение с [сопоставителем столбцов](../../sql-reference/statements/select/index.md#asterisk), например `*`, и/или [модификаторами](../../sql-reference/statements/select/index.md#select-modifiers), такими как [APPLY](/ru/sql-reference/statements/select/apply-modifier), [EXCEPT](/ru/sql-reference/statements/select/except-modifier), [REPLACE](/ru/sql-reference/statements/select/replace-modifier).

Например, рассмотрим таблицу:

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

Если вы хотите вставить данные во все столбцы, кроме столбца `b`, это можно сделать с помощью ключевого слова `EXCEPT`. Как видно из синтаксиса выше, число вставляемых значений (`VALUES (v11, v13)`) должно совпадать с числом указанных столбцов (`(c1, c3)`) :

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

В этом примере видно, что во второй вставленной строке столбцы `a` и `c` заполнены переданными значениями, а `b` — значением по умолчанию. Для вставки значений по умолчанию также можно использовать ключевое слово `DEFAULT`:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

Если список столбцов не включает все существующие столбцы, оставшиеся столбцы заполняются:

* Значениями, вычисленными на основе выражений `DEFAULT`, указанных в определении таблицы.
* Нулями и пустыми строками, если выражения `DEFAULT` не заданы.

Данные можно передавать в INSERT в любом [формате](/ru/sql-reference/formats), поддерживаемом ClickHouse. Формат должен быть явно указан в запросе:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

Например, следующий формат запроса совпадает с базовой версией `INSERT ... VALUES`:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse удаляет все пробелы и один символ перевода строки (если он есть) перед данными. При формировании запроса рекомендуем размещать данные на новой строке после операторов запроса — это особенно важно, если данные начинаются с пробелов.

Пример:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Вы можете выполнять вставку данных отдельно от запроса, используя [клиент командной строки](/ru/operations/utilities/clickhouse-local) или [HTTP-интерфейс](/ru/interfaces/http).

:::note
Если вы хотите указать `SETTINGS` для запроса `INSERT`, это нужно сделать *до* предложения `FORMAT`, поскольку всё после `FORMAT format_name` рассматривается как данные. Например:

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## Ограничения
</div>

Если у таблицы есть [ограничения](../../sql-reference/statements/create/table.md#constraints), их выражения будут проверяться для каждой строки вставляемых данных. Если какое-либо из этих ограничений нарушено, сервер сгенерирует исключение с именем ограничения и его выражением, а выполнение запроса будет остановлено.

<div id="data-type-validation">
  ## Проверка типов данных
</div>

ClickHouse проверяет допустимые типы данных (контролируемые такими настройками, как `enable_time_time64_type`, `allow_suspicious_low_cardinality_types`, `allow_suspicious_fixed_string_types` и т. д.) только при создании таблицы (`CREATE TABLE`) и изменении схемы (`ALTER TABLE`), а не во время `INSERT`.

Это означает, что если таблица с недопустимым типом данных уже существует, в неё можно вставлять данные, даже если соответствующая настройка отключена на сервере. Это сделано намеренно: после создания таблицы операции вставки не должны блокироваться настройками, управляющими созданием типов.

Например:

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
Как следствие, клиент с более новой версией (где эта настройка включена по умолчанию) может выполнять вставку данных с недопустимыми типами данных на сервер со старой версией (где эта настройка отключена), если в целевой таблице уже есть соответствующие типы столбцов. Проверка выполняется на уровне DDL, а не DML.
:::

<div id="inserting-the-results-of-select">
  ## Вставка результатов запроса SELECT
</div>

**Синтаксис**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

Столбцы сопоставляются в соответствии с их позицией в предложении `SELECT`. Однако их имена в выражении `SELECT` и в таблице для `INSERT` могут отличаться. При необходимости выполняется приведение типов.

Ни один из форматов данных, кроме формата Values, не позволяет задавать значения в виде выражений, таких как `now()`, `1 + 2` и так далее. Формат Values допускает ограниченное использование выражений, но это не рекомендуется, поскольку в этом случае для их выполнения используется неэффективный код.

Другие запросы на изменение частей данных не поддерживаются: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Однако старые данные можно удалить с помощью `ALTER TABLE ... DROP PARTITION`.

Предложение `FORMAT` должно быть указано в конце запроса, если предложение `SELECT` содержит табличную функцию [input()](../../sql-reference/table-functions/input.md).

Чтобы вставить значение по умолчанию вместо `NULL` в столбец с типом данных, не допускающим `NULL`, включите настройку [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default).

`INSERT` также поддерживает CTE (общее табличное выражение). Например, следующие два оператора эквивалентны:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## Вставка данных из файла
</div>

**Синтаксис**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

Используйте приведённый выше синтаксис, чтобы выполнить вставку данных из файла или файлов, расположенных на стороне **клиента**. `file_name` и `type` — строковые литералы. Входной [формат](../../interfaces/formats.md) файла должен быть задан в предложении `FORMAT`.

Поддерживаются сжатые файлы. Тип сжатия определяется по расширению имени файла. Его также можно явно указать в предложении `COMPRESSION`. Поддерживаются следующие типы: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

Эта возможность доступна в [клиенте командной строки](../../interfaces/client.md) и [clickhouse-local](../../operations/utilities/clickhouse-local.md).

**Примеры**

<div id="single-file-with-from-infile">
  ### Один файл через FROM INFILE
</div>

Выполните следующие запросы с помощью [клиента командной строки](../../interfaces/client.md):

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### Несколько файлов в FROM INFILE с использованием глоб-шаблонов
</div>

Этот пример очень похож на предыдущий, но в этом случае вставка выполняется из нескольких файлов через `FROM INFILE 'input_*.csv`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
Помимо выбора нескольких файлов с помощью `*`, можно использовать диапазоны (`{1,2}` или `{1..9}`) и другие [глоб-подстановки](/ru/sql-reference/table-functions/file.md/#globs-in-path). Все три варианта подойдут для приведенного выше примера:

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## Вставка с помощью табличной функции
</div>

Данные можно вставлять в таблицы, на которые указывают [табличные функции](../../sql-reference/table-functions/index.md).

**Синтаксис**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Пример**

Табличная функция [remote](/ru/sql-reference/table-functions/remote) используется в следующих запросах:

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## Вставка в ClickHouse Cloud
</div>

По умолчанию сервисы в ClickHouse Cloud предоставляют несколько реплик для высокой доступности. Когда вы подключаетесь к сервису, устанавливается соединение с одной из этих реплик.

После успешного выполнения `INSERT` данные записываются в нижележащее хранилище. Однако репликам может потребоваться некоторое время, чтобы получить эти обновления. Поэтому, если вы используете другое соединение, которое выполняет запрос `SELECT` к одной из других реплик, обновлённые данные могут там ещё не отображаться.

Можно использовать `select_sequential_consistency`, чтобы принудительно заставить реплику получить последние обновления. Вот пример запроса `SELECT` с использованием этой настройки:

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

Обратите внимание, что использование `select_sequential_consistency` увеличит нагрузку на ClickHouse Keeper (который ClickHouse Cloud использует внутри) и в зависимости от нагрузки на сервис может привести к снижению производительности. Мы не рекомендуем включать этот параметр без необходимости. Рекомендуемый подход — выполнять чтение и запись в рамках одного сеанса или использовать клиентский драйвер, который работает через собственный протокол (и, следовательно, поддерживает sticky-соединения).

<div id="inserting-into-a-replicated-setup">
  ## Вставка данных в реплицированной конфигурации
</div>

В реплицированной конфигурации данные становятся видимыми на других репликах только после завершения репликации. Репликация данных (их загрузка на другие реплики) начинается сразу после `INSERT`. Это отличается от ClickHouse Cloud, где данные сразу записываются в общее хранилище, а реплики отслеживают изменения метаданных.

Обратите внимание: в реплицированных конфигурациях `INSERTs` иногда могут занимать заметное время (порядка секунды), поскольку для этого требуется коммит в ClickHouse Keeper для достижения распределенного консенсуса. Использование S3 в качестве хранилища также увеличивает задержку.

<div id="performance-considerations">
  ## Особенности производительности
</div>

`INSERT` сортирует входные данные по первичному ключу и разбивает их на партиции по ключу партиционирования. Если вы вставляете данные сразу в несколько партиций, это может значительно снизить производительность запроса `INSERT`. Чтобы этого избежать:

* Добавляйте данные достаточно большими батчами, например по 100 000 строк за раз.
* Группируйте данные по ключу партиционирования перед загрузкой в ClickHouse.

Снижения производительности не будет, если:

* Данные поступают в реальном времени.
* Вы загружаете данные, которые, как правило, уже отсортированы по времени.

<div id="asynchronous-inserts">
  ### Асинхронные вставки
</div>

Данные можно вставлять асинхронно — небольшими, но частыми порциями. Данные из таких вставок объединяются в батчи, а затем безопасно вставляются в таблицу. Чтобы использовать асинхронные вставки, включите настройку [`async_insert`](/ru/operations/settings/settings#async_insert).

Использование `async_insert` или [движка таблицы `Buffer`](/ru/engines/table-engines/special/buffer) приводит к дополнительной буферизации.

<div id="large-or-long-running-inserts">
  ### Крупные или длительные вставки
</div>

При вставке больших объёмов данных ClickHouse оптимизирует производительность записи с помощью процесса, называемого &quot;squashing&quot;. Небольшие блоки данных в памяти объединяются и укрупняются перед записью на диск. Squashing уменьшает накладные расходы, связанные с каждой операцией записи. В ходе этого процесса вставленные данные становятся доступными для запросов после того, как ClickHouse завершит запись каждых [`max_insert_block_size`](/ru/operations/settings/settings#max_insert_block_size) строк.

**См. также**

* [async&#95;insert](/ru/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/ru/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/ru/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/ru/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/ru/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/ru/operations/settings/settings#async_insert_max_data_size)