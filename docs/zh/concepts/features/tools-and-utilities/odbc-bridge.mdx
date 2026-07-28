---
description: 'ODBC bridge 文档'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

这是一个简单的 HTTP 服务器，用作 ODBC Driver 的代理。其主要目的是
避免 ODBC 实现中可能出现的 segfault 或其他故障，因为这些问题可能导致整个
clickhouse-server 服务器进程崩溃。

该工具之所以通过 HTTP 工作，而不是通过管道、共享内存或 TCP，是因为：

* 更容易实现
* 更容易调试
* jdbc-bridge 也可以用同样的方式实现

<div id="usage">
  ## 用法
</div>

`clickhouse-server` 在 odbc table function 和 StorageODBC 中使用此工具。
不过，它也可以作为独立工具通过命令行使用，并在 POST 请求的 URL 中指定以下
参数：

* `connection_string` -- ODBC 连接字符串。
* `sample_block` -- 采用 ClickHouse NamesAndTypesList 格式的列描述，名称用反引号括起，
  类型为字符串。名称和类型之间以空格分隔，不同行之间以
  换行符分隔。
* `max_block_size` -- 可选参数，用于设置单个块的最大大小。
  查询通过 POST 请求体发送。返回结果采用 RowBinary format。

<div id="example">
  ## 示例：
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```