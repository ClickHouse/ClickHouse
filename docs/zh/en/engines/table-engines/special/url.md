---
description: '从远程 HTTP/HTTPS 服务器查询数据或向其写入数据。该引擎类似于
  File 表引擎。'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'URL 表引擎'
doc_type: 'reference'
---

从远程 HTTP/HTTPS 服务器查询数据或向其写入数据。该引擎类似于 [File](../../../engines/table-engines/special/file.md) 表引擎。

语法：`URL(URL [,Format] [,CompressionMethod])`

* `URL` 参数必须符合统一资源定位符的结构。对于 `http`/`https` URL (默认后端) ，它必须指向使用 HTTP 或 HTTPS 的服务器，并且从服务器获取响应时不需要任何额外的请求头。带有已识别的非 HTTP 协议 (`file://`、`s3://`、`az://`、`hdfs://`、…) 的 URL 则会被转交给对应的引擎处理——请参见下文的[按 URL 协议分派](#scheme-dispatch)。

* `Format` 必须是 ClickHouse 可在 `SELECT` 查询中使用的格式，并且在必要时也可用于 `INSERT`。有关受支持格式的完整列表，请参见 [Formats](/zh/interfaces/formats#formats-overview)。

  如果未指定此参数，ClickHouse 会根据 `URL` 参数的后缀自动检测格式。如果 `URL` 参数的后缀与任何受支持的格式都不匹配，则创建表会失败。例如，对于引擎表达式 `URL('http://localhost/test.json')`，将应用 `JSON` 格式。

* `CompressionMethod` 表示是否应压缩 HTTP body。如果启用了压缩，URL 引擎发送的 HTTP packet 将包含 `'Content-Encoding'` 请求头，以指示所使用的压缩方法。

要启用压缩，请先确保 `URL` 参数所指向的远程 HTTP 端点支持相应的压缩算法。

支持的 `CompressionMethod` 必须是以下之一：

* gzip or gz
* deflate
* brotli or br
* lzma or xz
* zstd or zst
* lz4
* bz2
* snappy
* none
* auto

如果未指定 `CompressionMethod`，则默认值为 `auto`。这意味着 ClickHouse 会根据 `URL` 参数的后缀自动检测压缩方法。如果该后缀与上面列出的任一压缩方法匹配，则应用相应的压缩；否则不启用压缩。

例如，对于引擎表达式 `URL('http://localhost/test.gzip')`，将应用 `gzip` 压缩方法；而对于 `URL('http://localhost/test.fr')`，则不会启用压缩，因为后缀 `fr` 与上述任何压缩方法都不匹配。

<div id="scheme-dispatch">
  ## 按 URL 协议 分派
</div>

`URL` 引擎是对其他文件和对象存储引擎的统一封装：它会根据 URL 协议 分派到正确的 后端。`http`/`https` (以及任何无法识别的 协议) 由 `URL` 引擎自身处理；`file://` 由 [File 表引擎](../../../engines/table-engines/special/file.md) 引擎处理；`s3://`、`gs://`、`gcs://`、`oss://` 由 [S3](/zh/engines/table-engines/integrations/s3) 引擎处理；`az://`、`azure://`、`abfss://`、`abfs://` 由 [AzureBlobStorage](/zh/engines/table-engines/integrations/azureBlobStorage) 引擎处理；`hdfs://` 由 [HDFS](/zh/engines/table-engines/integrations/hdfs) 引擎处理。

只有那些无需额外配置即可由 S3 URI mapper 解析为具体端点的 S3 协议 (`s3`，以及 `gs`/`gcs`/`oss`) 才会被分派。其他兼容 S3 的供应商 协议 (`cos`、`obs`、`eos`、……) 是区域特定的，且没有默认的端点映射，因此将此类 URL 传给 `URL` 引擎时，会被视为无法识别的 协议 并报错；对于这些 后端，请直接使用 [S3](/zh/engines/table-engines/integrations/s3) 引擎 (并配置 `url_scheme_mappers`) 。

[url&#95;base](/zh/operations/settings/settings.md#url_base) setting 会在 协议 分派前应用，因此相对引用会先基于 base 解析，再路由到匹配的引擎。

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## 用法
</div>

`INSERT` 和 `SELECT` 查询分别会转换为 `POST` 和 `GET` 请求。
要处理 `POST` 请求，远程服务器必须支持
[分块传输编码](https://en.wikipedia.org/wiki/Chunked_transfer_encoding)。

你可以使用 [max&#95;http&#95;get&#95;redirects](/zh/operations/settings/settings#max_http_get_redirects) 设置来限制 HTTP GET 重定向的最大跳转次数。

<div id="wildcards-with-http-index-pages">
  ## HTTP 索引页中的通配符
</div>

启用 [allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/zh/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages) 后，`URL` 表引擎可以通过拉取 HTTP 索引页并从中提取链接来扩展通配符。
这与 [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages) 表函数使用的机制相同。

扩展会受到以下限制：每个拉取到的索引页受 [max&#95;http&#95;index&#95;page&#95;size](/zh/operations/server-configuration-parameters/settings.md#max_http_index_page_size) 限制；递归遍历目录时，受 [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/zh/operations/settings/settings.md#url_wildcard_max_directories_to_read) 限制。

<div id="example">
  ## 示例
</div>

**1.** 在服务器上创建 `url_engine_table` 表：

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** 使用 Python 3 自带的标准工具创建一个简单的 HTTP 服务器，并
启动它：

```python3
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

```bash
$ python3 server.py
```

**3.** 请求数据：

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## 实现细节
</div>

* 读写可并行进行
* 不支持：
  * `ALTER` 和 `SELECT...SAMPLE` 操作。
  * 索引。
  * 复制。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — `URL` 的路径。类型：`LowCardinality(String)`。
* `_file` — `URL` 的资源名。类型：`LowCardinality(String)`。
* `_size` — 资源大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_headers` - HTTP 响应头。类型：`Map(LowCardinality(String), LowCardinality(String))`。

<div id="resolving-relative-urls">
  ## 解析相对 URL
</div>

[url&#95;base](/zh/operations/settings/settings.md#url_base) 设置允许在 `URL` 引擎中使用相对 URL。设置 `url_base` 后，传递给该引擎的 URL 会根据 [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986) 相对于该基准 URL 进行解析。有关解析规则的完整说明，请参阅 [url 表函数文档](../../../sql-reference/table-functions/url.md#resolving-relative-urls)。

**示例**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## 存储设置
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/zh/operations/settings/settings.md#engine_url_skip_empty_files) - 允许在读取时跳过空文件。默认禁用。
* [enable&#95;url&#95;encoding](/zh/operations/settings/settings.md#enable_url_encoding) - 允许启用/禁用 URI 中路径的解码/编码。默认启用。
* [url&#95;base](/zh/operations/settings/settings.md#url_base) - 用于解析传递给引擎的相对 URL 的基础 URL。