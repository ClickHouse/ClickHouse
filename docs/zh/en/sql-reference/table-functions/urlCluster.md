---
description: '允许在指定
  集群中由多个节点并行处理来自 URL 的文件。'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

允许在指定集群中由多个节点并行处理来自 URL 的文件。在发起节点上，它会与集群中的所有节点建立连接，展开 URL 文件路径中的 asterisk，并动态分发每个文件。在工作节点上，它会向发起节点请求下一个要处理的任务并进行处理。重复此过程，直到所有任务都完成。

<div id="syntax">
  ## 语法
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## 参数
</div>

| 参数             | 说明                                                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| `cluster_name` | 用于构建远程和本地服务器地址集合及连接参数的集群名称。                                                                            |
| `URL`          | 可接受 `GET` 请求的 HTTP 或 HTTPS 服务器地址。类型：[String](../../sql-reference/data-types/string.md)。                |
| `format`       | 数据的[格式](/zh/sql-reference/formats)。类型：[String](../../sql-reference/data-types/string.md)。                 |
| `structure`    | `'UserID UInt64, Name String'` 格式的表结构，用于确定列名和类型。类型：[String](../../sql-reference/data-types/string.md)。 |

<div id="returned_value">
  ## 返回值
</div>

一个具有指定格式和结构，并包含来自指定 `URL` 的数据的表。

<div id="examples">
  ## 示例
</div>

从一个以 [CSV](/zh/interfaces/formats/CSV) 格式响应的 HTTP 服务器中，获取包含 `String` 和 [UInt32](../../sql-reference/data-types/int-uint.md) 类型列的表的前 3 行。

1. 使用标准 Python 3 工具创建一个简单的 HTTP 服务器并启动：

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## URL 中的通配符
</div>

`{ }` 中的模式可用于生成一组分片，或指定故障转移地址。支持的模式类型和示例，请参见 [remote](remote.md#globs-in-addresses) 函数的说明。
模式中的 `|` 字符用于指定故障转移地址。系统会按它们在模式中列出的顺序依次尝试。生成的地址数量受 [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements) 设置限制。

<div id="related">
  ## 相关
</div>

* [HDFS 引擎](/zh/engines/table-engines/integrations/hdfs)
* [URL 表函数](/zh/engines/table-engines/special/url)