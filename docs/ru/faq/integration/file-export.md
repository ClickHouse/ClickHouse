---
title: Как экспортировать данные из ClickHouse в файл?
sidebar_position: 10
---

# Как экспортировать данные из ClickHouse в файл? {#how-to-export-to-file-rus}

## Секция INTO OUTFILE {#using-into-outfile-clause}

Добавьте к своему запросу секцию [INTO OUTFILE](../../sql-reference/statements/select/into-outfile.md#into-outfile-clause).

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file';
```

По умолчанию при выдаче данных ClickHouse использует формат [TabSeparated](../../interfaces/formats.md#tabseparated). Чтобы выбрать другой [формат данных](../../interfaces/formats.md), используйте секцию [FORMAT](../../sql-reference/statements/select/format.md#format-clause).

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV;
```

## Таблица с движком File {#using-a-file-engine-table}

Смотрите [File](../../engines/table-engines/special/file.md).

## Перенаправление в командой строке {#using-command-line-redirection}

``` bash
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Смотрите [clickhouse-client](../../interfaces/cli.md).
