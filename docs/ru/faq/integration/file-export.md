---
title: How do I export data from ClickHouse to a file?
toc_hidden: true
toc_priority: 10
---

## Как экспортировать данные из ClickHouse в файл? {#how-to-export-to-file-rus}

### Секция INTO OUTFILE {#sektsiia-into-outfile-rus}

Добавьте секцию [INTO OUTFILE](../../sql-reference/statements/select/into-outfile.md#into-outfile-clause) к своему запросу.

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

По умолчанию, для выдачи данных ClickHouse использует формат [TabSeparated](../../interfaces/formats.md#tabseparated). Чтобы выбрать [формат данных](../../interfaces/formats.md), используйте секцию [FORMAT](../../sql-reference/statements/select/format.md#format-clause).

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

## Таблица с движком File {#using-a-file-engine-table}

Смотрите [File](../../engines/table-engines/special/file.md).

## Перенаправление в командой строке {#using-command-line-redirection}

``` bash
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Смотрите [clickhouse-client](../../interfaces/cli.md).
