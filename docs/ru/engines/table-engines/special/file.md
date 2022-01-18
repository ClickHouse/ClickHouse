---
toc_priority: 37
toc_title: File
---

# File(Format) {#table_engines-file}

Управляет данными в одном файле на диске в указанном формате.

Примеры применения:

-   Выгрузка данных из ClickHouse в файл.
-   Преобразование данных из одного формата в другой.
-   Обновление данных в ClickHouse редактированием файла на диске.

## Использование движка в сервере ClickHouse {#ispolzovanie-dvizhka-v-servere-clickhouse}

``` sql
File(Format)
```

`Format` должен быть таким, который ClickHouse может использовать и в запросах `INSERT` и в запросах `SELECT`. Полный список поддерживаемых форматов смотрите в разделе [Форматы](../../../interfaces/formats.md#formats).

Сервер ClickHouse не позволяет указать путь к файлу, с которым будет работать `File`. Используется путь к хранилищу, определенный параметром [path](../../../operations/server-configuration-parameters/settings.md) в конфигурации сервера.

При создании таблицы с помощью `File(Format)` сервер ClickHouse создает в хранилище каталог с именем таблицы, а после добавления в таблицу данных помещает туда файл `data.Format`.

Можно вручную создать в хранилище каталог таблицы, поместить туда файл, затем на сервере ClickHouse добавить ([ATTACH](../../../engines/table-engines/special/file.md)) информацию о таблице, соответствующей имени каталога и прочитать из файла данные.

!!! warning "Warning"
    Будьте аккуратны с этой функциональностью, поскольку сервер ClickHouse не отслеживает внешние изменения данных. Если в файл будет производиться запись одновременно со стороны сервера ClickHouse и с внешней стороны, то результат непредсказуем.

**Пример:**

**1.** Создадим на сервере таблицу `file_engine_table`:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

В конфигурации по умолчанию сервер ClickHouse создаст каталог `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Вручную создадим файл `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` с содержимым:

``` bash
$cat data.TabSeparated
one 1
two 2
```

**3.** Запросим данные:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Использование движка в Clickhouse-local {#ispolzovanie-dvizhka-v-clickhouse-local}

В [clickhouse-local](../../../engines/table-engines/special/file.md) движок в качестве параметра принимает не только формат, но и путь к файлу. В том числе можно указать стандартные потоки ввода/вывода цифровым или буквенным обозначением `0` или `stdin`, `1` или `stdout`. Можно записывать и читать сжатые файлы. Для этого нужно задать дополнительный параметр движка или расширение файла (`gz`, `br` или `xz`).

**Пример:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## Детали реализации {#detali-realizatsii}

-   Поддерживается одновременное выполнение множества запросов `SELECT`, запросы `INSERT` могут выполняться только последовательно.
-   Поддерживается создание ещё не существующего файла при запросе `INSERT`.
-   Для существующих файлов `INSERT` записывает в конец файла.
-   Не поддерживается:
    -   использование операций `ALTER` и `SELECT...SAMPLE`;
    -   индексы;
    -   репликация.

