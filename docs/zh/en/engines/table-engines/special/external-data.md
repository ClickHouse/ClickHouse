---
description: 'ClickHouse 支持在发送 `SELECT` 查询时，一并将查询处理所需的数据发送到服务器。这些数据会被放入临时表中，并可在查询中使用（例如在 `IN` 运算符中）。'
sidebar_label: '查询处理的外部数据'
sidebar_position: 130
slug: /engines/table-engines/special/external-data
title: '查询处理的外部数据'
doc_type: 'reference'
---

ClickHouse 支持在发送 `SELECT` 查询时，一并将查询处理所需的数据发送到服务器。这些数据会被放入临时表中 (参见“临时表”一节) ，并可在查询中使用 (例如在 `IN` 运算符中) 。

例如，如果有一个包含重要用户标识符的文本文件，可以将它连同使用该列表进行过滤的查询一起上传到服务器。

如果需要使用大量外部数据运行多个查询，请不要使用此功能。更好的做法是提前将数据上传到 DB。

外部数据可以通过命令行客户端 (以非交互模式) 上传，也可以通过 HTTP 接口上传。

在命令行客户端中，可以按以下格式指定参数部分

```bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

你可能会有多个这样的节，对应传输的表数量。

**–external** – 标记一个子句的开始。
**–file** – 包含表转储的文件路径，或 `-`，表示 stdin。
只能从 stdin 检索单个表。

以下参数是可选的：**–name**– 表名。如果省略，则使用 &#95;data。
**–format** – 文件中的数据格式。如果省略，则使用 TabSeparated。

以下参数中必须指定一个：**–types** – 以逗号分隔的列类型列表。例如：`UInt64,String`。这些列将被命名为 &#95;1、&#95;2、...
**–structure**– 格式为 `UserID UInt64`、`URL String` 的表结构。用于定义列名和类型。

在 &#39;file&#39; 中指定的文件将按 &#39;format&#39; 中指定的格式进行解析，并使用 &#39;types&#39; 或 &#39;structure&#39; 中指定的数据类型。该表将被上传到服务器，并可在服务器上作为名为 &#39;name&#39; 的临时表访问。

示例：

```bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

使用 HTTP 接口时，外部数据通过 multipart/form-data 格式传递。每个表都会作为单独的文件传输，表名取自文件名。`query_string` 中会传递参数 `name_format`、`name_types` 和 `name_structure`，其中 `name` 是这些参数对应的表名。这些参数的含义与使用命令行客户端时相同。

示例：

```bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

在分布式查询处理中，临时表会发送到所有远程服务器。