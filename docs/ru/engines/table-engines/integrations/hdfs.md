---
toc_priority: 4
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

Управляет данными в HDFS. Данный движок похож на движки [File](../special/file.md#table_engines-file) и [URL](../special/url.md#table_engines-url).

## Использование движка {#ispolzovanie-dvizhka}

``` sql
ENGINE = HDFS(URI, format)
```

В параметр `URI` нужно передавать полный URI файла в HDFS.
Параметр `format` должен быть таким, который ClickHouse может использовать и в запросах `INSERT`, и в запросах `SELECT`. Полный список поддерживаемых форматов смотрите в разделе [Форматы](../../../interfaces/formats.md#formats).
Часть URI с путем файла может содержать шаблоны. В этом случае таблица может использоваться только для чтения.

**Пример:**

**1.** Создадим на сервере таблицу `hdfs_engine_table`:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Заполним файл:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Запросим данные:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Детали реализации {#detali-realizatsii}

-   Поддерживается многопоточное чтение и запись.
-   Не поддерживается:
    -   использование операций `ALTER` и `SELECT...SAMPLE`;
    -   индексы;
    -   репликация.

**Шаблоны в пути**

Шаблоны могут содержаться в нескольких компонентах пути. Обрабатываются только существующие файлы, название которых целиком удовлетворяет шаблону (не только суффиксом или префиксом).

-   `*` — Заменяет любое количество любых символов кроме `/`, включая отсутствие символов.
-   `?` — Заменяет ровно один любой символ.
-   `{some_string,another_string,yet_another_one}` — Заменяет любую из строк `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Заменяет любое число в интервале от `N` до `M` включительно (может содержать ведущие нули).

Конструкция с `{}` аналогична табличной функции [remote](../../../engines/table-engines/integrations/hdfs.md).

**Пример**

1.  Предположим, у нас есть несколько файлов со следующими URI в HDFS:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

1.  Есть несколько возможностей создать таблицу, состояющую из этих шести файлов:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Другой способ:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Таблица, состоящая из всех файлов в обеих директориях (все файлы должны удовлетворять формату и схеме, указанной в запросе):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "Warning"
    Если список файлов содержит числовые интервалы с ведущими нулями, используйте конструкцию с фигурными скобочками для каждой цифры или используйте `?`.

**Example**

Создадим таблицу с именами `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## Виртуальные столбцы {#virtualnye-stolbtsy}

-   `_path` — Путь к файлу.
-   `_file` — Имя файла.

**Смотрите также**

-   [Виртуальные столбцы](index.md#table_engines-virtual_columns)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/hdfs/) <!--hide-->
