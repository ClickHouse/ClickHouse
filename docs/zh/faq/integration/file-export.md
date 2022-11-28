---
title: 如何从 ClickHouse 导出数据到一个文件?
toc_hidden: true
toc_priority: 10
---

# 如何从 ClickHouse 导出数据到一个文件? {#how-to-export-to-file}

## 使用 INTO OUTFILE 语法 {#using-into-outfile-clause}

加一个 [INTO OUTFILE](../../sql-reference/statements/select/into-outfile.md#into-outfile-clause) 语法到你的查询语句中.

例如:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

ClickHouse 默认使用[TabSeparated](../../interfaces/formats.md#tabseparated) 格式写入数据. 修改[数据格式](../../interfaces/formats.md), 请用 [FORMAT 语法](../../sql-reference/statements/select/format.md#format-clause).

例如:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

## 使用一个文件引擎表 {#using-a-file-engine-table}

查看 [File](../../engines/table-engines/special/file.md) 表引擎.

## 使用命令行重定向 {#using-command-line-redirection}

``` bash
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

查看 [clickhouse-client](../../interfaces/cli.md).
