---
description: 'INTO OUTFILE 子句文档'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'INTO OUTFILE 子句'
doc_type: 'reference'
---

`INTO OUTFILE` 子句会将 `SELECT` 查询结果重定向到**客户端**侧的文件中。

支持压缩文件。压缩类型会根据文件扩展名自动检测 (默认使用 `'auto'` 模式) ，也可以在 `COMPRESSION` 子句中显式指定。还可以在 `LEVEL` 子句中指定特定压缩类型的压缩级别。

**语法**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` 和 `type` 是字符串字面量。支持的压缩类型有：`'none'`、`'gzip'`、`'deflate'`、`'br'`、`'xz'`、`'zstd'`、`'lz4'`、`'bz2'`。

`level` 是数字字面量。支持以下范围内的正整数：`lz4` 类型为 `1-12`，`zstd` 类型为 `1-22`，其他压缩类型为 `1-9`。

<div id="implementation-details">
  ## 实现细节
</div>

* 此功能可在[命令行客户端](../../../interfaces/client.md)和[clickhouse-local](../../../operations/utilities/clickhouse-local.md)中使用。因此，通过[HTTP 接口](/zh/interfaces/http)发送的查询会失败。
* 如果已存在同名文件，查询会失败。
* 默认[输出格式](../../../interfaces/formats.md)为 `TabSeparated` (与命令行客户端的批次模式相同) 。使用 [FORMAT](format.md) 子句可更改。
* 如果查询中指定了 `AND STDOUT`，则写入文件的输出也会显示到标准输出上。如果同时使用压缩，则标准输出上显示的是明文。
* 如果查询中指定了 `APPEND`，则输出会追加到现有文件中。如果使用压缩，则不能使用 `APPEND`。
* 当写入已存在的文件时，必须使用 `APPEND` 或 `TRUNCATE`。

**示例**

使用[命令行客户端](../../../interfaces/client.md)执行以下查询：

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```