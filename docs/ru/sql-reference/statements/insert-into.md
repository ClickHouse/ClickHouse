---
sidebar_position: 33
sidebar_label: INSERT INTO
---

## INSERT INTO {#insert}

Добавляет данные в таблицу.

**Синтаксис**

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

Вы можете указать список столбцов для вставки, используя синтаксис `(c1, c2, c3)`. Также можно использовать выражение cо [звездочкой](../../sql-reference/statements/select/index.md#asterisk) и/или модификаторами, такими как [APPLY](../../sql-reference/statements/select/index.md#apply-modifier), [EXCEPT](../../sql-reference/statements/select/index.md#except-modifier), [REPLACE](../../sql-reference/statements/select/index.md#replace-modifier).

В качестве примера рассмотрим таблицу:

``` sql
SHOW CREATE insert_select_testtable
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

``` sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1)
```

Если вы хотите вставить данные во все столбцы, кроме 'b', вам нужно передать столько значений, сколько столбцов вы указали в скобках:

``` sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2)
```

``` sql
SELECT * FROM insert_select_testtable
```

```
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

В этом примере мы видим, что вторая строка содержит столбцы `a` и `c`, заполненные переданными значениями и `b`, заполненный значением по умолчанию. Также можно использовать ключевое слово `DEFAULT` для вставки значений по умолчанию:

``` sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

Если список столбцов не включает все существующие столбцы, то все остальные столбцы заполняются следующим образом:

-   Значения, вычисляемые из `DEFAULT` выражений, указанных в определении таблицы.
-   Нули и пустые строки, если `DEFAULT` не определены.

В INSERT можно передавать данные любого [формата](../../interfaces/formats.md#formats), который поддерживает ClickHouse. Для этого формат необходимо указать в запросе в явном виде:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

Например, следующий формат запроса идентичен базовому варианту INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse отсекает все пробелы и один перенос строки (если он есть) перед данными. Рекомендуем при формировании запроса переносить данные на новую строку после операторов запроса (это важно, если данные начинаются с пробелов).

Пример:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

С помощью консольного клиента или HTTP интерфейса можно вставлять данные отдельно от запроса. Как это сделать, читайте в разделе «[Интерфейсы](../../interfaces/index.md#interfaces)».

### Ограничения (constraints) {#ogranicheniia-constraints}

Если в таблице объявлены [ограничения](../../sql-reference/statements/create/table.md#constraints), то их выполнимость будет проверена для каждой вставляемой строки. Если для хотя бы одной строки ограничения не будут выполнены, запрос будет остановлен.

### Вставка результатов `SELECT` {#insert_query_insert-select}

**Синтаксис**

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Соответствие столбцов определяется их позицией в секции SELECT. При этом, их имена в выражении SELECT и в таблице для INSERT, могут отличаться. При необходимости выполняется приведение типов данных, эквивалентное соответствующему оператору CAST.

Все форматы данных кроме Values не позволяют использовать в качестве значений выражения, такие как `now()`, `1 + 2` и подобные. Формат Values позволяет ограниченно использовать выражения, но это не рекомендуется, так как в этом случае для их выполнения используется неэффективный вариант кода.

Не поддерживаются другие запросы на модификацию части данных: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Вы можете удалять старые данные с помощью запроса `ALTER TABLE ... DROP PARTITION`.

Для табличной функции [input()](../table-functions/input.md) после секции `SELECT` должна следовать
секция `FORMAT`.

Чтобы вставить значение по умолчанию вместо `NULL` в столбец, который не позволяет хранить `NULL`, включите настройку [insert_null_as_default](../../operations/settings/settings.md#insert_null_as_default).

### Вставка данных из файла {#inserting-data-from-a-file}

**Синтаксис**

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] FORMAT format_name
```

Используйте этот синтаксис, чтобы вставить данные из файла, который хранится на стороне **клиента**. `file_name` и `type` задаются в виде строковых литералов. [Формат](../../interfaces/formats.md) входного файла должен быть задан в секции `FORMAT`. 

Поддерживаются сжатые файлы. Формат сжатия определяется по расширению файла, либо он может быть задан в секции `COMPRESSION`. Поддерживаются форматы: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

Эта функциональность поддерживается [клиентом командной строки](../../interfaces/cli.md) и [clickhouse-local](../../operations/utilities/clickhouse-local.md).

**Пример**

Выполните следующие запросы, используя [клиент командной строки](../../interfaces/cli.md):

```bash
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

Результат:

```text
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

### Вставка в табличную функцию {#inserting-into-table-function}

Данные могут быть вставлены в таблицы, заданные с помощью [табличных функций](../../sql-reference/table-functions/index.md).

**Синтаксис**
``` sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Пример**

Табличная функция [remote](../../sql-reference/table-functions/index.md#remote) используется в следующих запросах:

``` sql
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table) 
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

Результат:

``` text
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

### Замечания о производительности {#zamechaniia-o-proizvoditelnosti}

`INSERT` сортирует входящие данные по первичному ключу и разбивает их на партиции по ключу партиционирования. Если вы вставляете данные в несколько партиций одновременно, то это может значительно снизить производительность запроса `INSERT`. Чтобы избежать этого:

-   Добавляйте данные достаточно большими пачками. Например, по 100 000 строк.
-   Группируйте данные по ключу партиционирования самостоятельно перед загрузкой в ClickHouse.

Снижения производительности не будет, если:

-   Данные поступают в режиме реального времени.
-   Вы загружаете данные, которые как правило отсортированы по времени.

Также возможно вставлять данные асинхронно во множественных маленьких вставках. Данные от таких вставок сначала собираются в пачки, а потом вставляются в таблицу. Чтобы включить асинхронный режим, используйте настройку [async_insert](../../operations/settings/settings.md#async-insert). Обратите внимание, что асинхронные вставки поддерживаются только через протокол HTTP, а дедупликация при этом не производится.

**См. также**

-   [async_insert](../../operations/settings/settings.md#async-insert)
-   [async_insert_threads](../../operations/settings/settings.md#async-insert-threads)
-   [wait_for_async_insert](../../operations/settings/settings.md#wait-for-async-insert)
-   [wait_for_async_insert_timeout](../../operations/settings/settings.md#wait-for-async-insert-timeout)
-   [async_insert_max_data_size](../../operations/settings/settings.md#async-insert-max-data-size)
-   [async_insert_busy_timeout_ms](../../operations/settings/settings.md#async-insert-busy-timeout-ms)
-   [async_insert_stale_timeout_ms](../../operations/settings/settings.md#async-insert-stale-timeout-ms)
