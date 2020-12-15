---
toc_folder_title: F.A.Q.
toc_hidden: true
toc_priority: 76
---

# Общие вопросы {#obshchie-voprosy}

This section of the documentation is a place to collect answers to ClickHouse-related questions that arise often.

Categories:

-   **[General](../faq/general/index.md)**
    -   [What is ClickHouse?](../index.md#what-is-clickhouse)
    -   [Why ClickHouse is so fast?](../faq/general/why-clickhouse-is-so-fast.md)
    -   [Who is using ClickHouse?](../faq/general/who-is-using-clickhouse.md)
    -   [What does “ClickHouse” mean?](../faq/general/dbms-naming.md)
    -   [What does “Не тормозит” mean?](../faq/general/ne-tormozit.md)
    -   [What is OLAP?](../faq/general/olap.md)
    -   [What is a columnar database?](../faq/general/columnar-database.md)
    -   [Почему бы не использовать системы типа MapReduce?](../faq/general/mapreduce.md)
-   **[Use Cases](../faq/use-cases/index.md)**
    -   [Can I use ClickHouse as a time-series database?](../faq/use-cases/time-series.md)
    -   [Can I use ClickHouse as a key-value storage?](../faq/use-cases/key-value.md)
-   **[Operations](../faq/operations/index.md)**
    -   [Which ClickHouse version to use in production?](../faq/operations/production.md)
    -   [Is it possible to delete old records from a ClickHouse table?](../faq/operations/delete-old-data.md)
-   **[Integration](../faq/integration/index.md)**
    -   [How do I export data from ClickHouse to a file?](../faq/integration/file-export.md)
    -   [What if I have a problem with encodings when connecting to Oracle via ODBC?](../faq/integration/oracle-odbc.md)

## Что делать, если у меня проблема с кодировками при использовании Oracle через ODBC? {#oracle-odbc-encodings}

Если вы используете Oracle через драйвер ODBC в качестве источника внешних словарей, необходимо задать правильное значение для переменной окружения `NLS_LANG` в `/etc/default/clickhouse`. Подробнее читайте в [Oracle NLS_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Пример**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## Как экспортировать данные из ClickHouse в файл? {#how-to-export-to-file}

### Секция INTO OUTFILE {#sektsiia-into-outfile}

Добавьте секцию [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) к своему запросу.

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

По умолчанию, для выдачи данных ClickHouse использует формат [TabSeparated](../interfaces/formats.md#tabseparated). Чтобы выбрать [формат данных](../interfaces/formats.md), используйте [секцию FORMAT](../sql-reference/statements/select/format.md#format-clause).

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### Таблица с движком File {#tablitsa-s-dvizhkom-file}

Смотрите [File](../engines/table-engines/special/file.md).

### Перенаправление в командой строке {#perenapravlenie-v-komandoi-stroke}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Смотрите [clickhouse-client](../interfaces/cli.md).

[Оригинальная статья](https://clickhouse.tech/docs/en/faq/general/) <!--hide-->
