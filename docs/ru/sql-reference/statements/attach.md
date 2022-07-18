---
toc_priority: 40
toc_title: ATTACH
---

# ATTACH {#attach}

Выполняет подключение таблицы или словаря, например, при перемещении базы данных на другой сервер.

**Синтаксис**

``` sql
ATTACH TABLE|DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

Запрос не создаёт данные на диске, а предполагает, что данные уже лежат в соответствующих местах, и всего лишь добавляет информацию о таблице или словаре на сервер. После выполнения запроса `ATTACH` сервер будет знать о существовании таблицы или словаря.

Если таблица перед этим была отключена при помощи ([DETACH](../../sql-reference/statements/detach.md)), т.е. её структура известна, можно использовать сокращенную форму записи без определения структуры.

## Присоединение существующей таблицы {#attach-existing-table}

**Синтаксис**

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Этот запрос используется при старте сервера. Сервер хранит метаданные таблиц в виде файлов с запросами `ATTACH`, которые он просто исполняет при запуске (за исключением некоторых системных таблиц, которые явно создаются на сервере).

Если таблица была отключена перманентно, она не будет подключена обратно во время старта сервера, так что нужно явно использовать запрос `ATTACH`, чтобы подключить ее.

## Создание новой таблицы и присоединение данных {#create-new-table-and-attach-data}

### С указанием пути к табличным данным {#attach-with-specified-path}

Запрос создает новую таблицу с указанной структурой и присоединяет табличные данные из соответствующего каталога в `user_files`.

**Синтаксис**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**Пример**

Запрос:

```sql
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```
Результат:

```sql
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

### С указанием UUID таблицы {#attach-with-specified-uuid}

Этот запрос создает новую таблицу с указанной структурой и присоединяет данные из таблицы с указанным UUID.
Запрос поддерживается только движком баз данных [Atomic](../../engines/database-engines/atomic.md).

**Синтаксис**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

## Присоединение существующего словаря {#attach-existing-dictionary}

Присоединяет ранее отключенный словарь.

**Синтаксис**

``` sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```
