---
description: '使用给定的 `URL`、`format` 和 `结构` 创建表'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # url 表函数
</div>

`url` function 使用给定的 `format` 和 `structure`，从 `URL` 创建表。

`url` function 可用于在 [URL](../../engines/table-engines/special/url.md) 表中的数据上执行 `SELECT` 和 `INSERT` 查询。

<div id="syntax">
  ## 语法
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## 参数
</div>

| 参数          | 描述                                                                                                                                                                                                                                                                       |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `URL`       | 用单引号括起来的 URL，其协议用于选择后端。`http`/`https` (或无法识别的) URL 是接受 `GET` 或 `POST` 请求的服务器地址 (分别对应 `SELECT` 或 `INSERT` 查询) ；可识别的非 HTTP 协议 (`file://`、`s3://`、`az://`、`hdfs://`、…) 会交由对应的表函数处理——参见[按 URL 协议分派](#scheme-dispatch)。类型：[String](../../sql-reference/data-types/string.md)。 |
| `format`    | 数据的[格式](/zh/sql-reference/formats)。类型：[String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                   |
| `structure` | `'UserID UInt64, Name String'` 格式的表结构。用于确定列名和类型。类型：[String](../../sql-reference/data-types/string.md)。                                                                                                                                                                   |
| `headers`   | `'headers('key1'='value1', 'key2'='value2')'` 格式的请求头。可为 HTTP 调用设置请求头。                                                                                                                                                                                                    |

<div id="returned_value">
  ## 返回值
</div>

一个具有指定格式和结构、且包含来自指定 `URL` 的数据的表。

<div id="examples">
  ## 示例
</div>

获取一个包含 `String` 和 [UInt32](../../sql-reference/data-types/int-uint.md) 类型列的表的前 3 行，该表来自以 [CSV](/zh/interfaces/formats/CSV) 格式响应的 HTTP 服务器。

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

将 `URL` 中的数据插入到表中：

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## 按 URL 协议分派
</div>

`url` 函数是对其他文件和对象存储表函数的统一封装：它会根据 URL 协议分派到相应的后端。这样，你就可以使用同一种语法从任何受支持的位置读取数据。

| 协议                                          | Dispatches to                                |
| --------------------------------------------- | -------------------------------------------- |
| `http`, `https` (and any unrecognized 协议) | `URL` 引擎本身 (HTTP `GET`/`POST`)               |
| `file`                                        | [`file`](file.md) 函数                         |
| `s3`, `gs`, `gcs`, `oss`                      | [`s3`](s3.md) 函数                             |
| `az`, `azure`, `abfss`, `abfs`                | [`azureBlobStorage`](azureBlobStorage.md) 函数 |
| `hdfs`                                        | [`hdfs`](hdfs.md) 函数                         |

只有那些可由 S3 URI mapper 在无需额外配置的情况下解析为具体端点的 S3 协议 (`s3`，以及 `gs`/`gcs`/`oss`) 才会被分派。其他兼容 S3 的厂商协议 (`cos`、`obs`、`eos` 等) 具有区域特定性，且没有默认端点映射，因此 `cos://…` URL 会被视为无法识别的协议并报错；对于这些后端，请直接使用 [`s3`](s3.md) 函数 (并配置 `url_scheme_mappers`) 。

对于 `file://`，相对路径 (`file://data.csv`) 会在 [user&#95;files](/zh/operations/server-configuration-parameters/settings#user_files_path) 目录内解析，而绝对路径 (`file:///home/user/data.csv`) 则和往常一样，必须指向该目录内部。

无论分派目标是什么，`format`、`structure` 和 `compression_method` 参数以及 [url&#95;base](#resolving-relative-urls) 设置的工作方式都相同。

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

协议分发尚未在 [`urlCluster`](urlCluster.md) 中完成支持：传递给 `urlCluster` 的非 `http(s)` 协议会被拒绝并报错。对于这些后端，请改用对应的 cluster 函数 (`s3Cluster`、`azureBlobStorageCluster`、`hdfsCluster`、…) 。

<div id="globs-in-url">
  ## URL 中的通配符
</div>

`{ }` 中的模式用于生成一组分片，或指定故障转移地址。有关支持的模式类型和示例，请参见 [remote](remote.md#globs-in-addresses) 函数的说明。
模式中的字符 `|` 用于指定故障转移地址。系统会按照模式中列出的顺序依次遍历这些地址。生成的地址数量受 [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements) 设置限制。
有关 URL 路径中的通配符语法 (例如 `*`、`{a,b}`、`{N..M}` 和 `**`) ，请参见 [路径中的通配符](file.md#globs-in-path)。请注意，在 URL 中，`?` 表示查询字符串的开始，因此不能在路径部分用作通配符。

<div id="wildcards-with-http-index-pages">
  ## HTTP 索引页中的通配符
</div>

对于 `url` 和 `URL` 表引擎，ClickHouse 可以通过拉取 HTTP 索引页 (HTML 或纯文本) 并从响应体中提取 URL 来展开通配符。当服务器提供目录列表时，这使得 `/**/` 之类的模式成为可能。

注意：

* 相对 URL 会相对于索引页 URL 进行解析。
* 在拉取索引页之前，会先展开 `URL` 模板，包括逗号分隔和数字范围的分片展开，以及路径部分之外的 `|` 故障转移选项。
* 路径部分内的 `|` 故障转移模式不支持用于 HTTP 索引页展开。
* 通配符匹配应用于 URL 的路径部分。
* 如果列出的 URL 已包含查询字符串或片段，则优先使用它，而不是源 URL 中的对应内容。否则，将使用源 URL 中的查询字符串和片段。
* 允许空列表；索引页返回 HTTP 错误 (例如 404) 时会引发异常。
* 索引页的最大大小受 [max&#95;http&#95;index&#95;page&#95;size](/zh/operations/server-configuration-parameters/settings.md#max_http_index_page_size) 限制。
* 递归展开期间可读取的最大目录数量受 [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/zh/operations/settings/settings.md#url_wildcard_max_directories_to_read) 限制。

示例：

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — `URL` 的路径。类型：`LowCardinality(String)`。
* `_file` — `URL` 的资源名称。类型：`LowCardinality(String)`。
* `_size` — 资源大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_headers` - HTTP 响应头。类型：`Map(LowCardinality(String), LowCardinality(String))`。

<div id="hive-style-partitioning">
  ## use_hive_partitioning 设置
</div>

当 `use_hive_partitioning` 设置为 1 时，ClickHouse 会检测路径中的 Hive 风格分区 (`/name=value/`) ，并允许在查询中将分区列作为虚拟列使用。这些虚拟列的名称将与分区路径中的名称相同。

**示例**

使用通过 Hive 风格分区生成的虚拟列

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## 解析相对 URL
</div>

[url&#95;base](/zh/operations/settings/settings.md#url_base) 设置允许向 `url` 函数传递相对 URL。当设置了 `url_base` 且函数参数为相对引用时，会根据 [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986) 以 base URL 为基准进行解析。

解析规则如下：

* **路径相对** (例如 `data.csv`) ：与 base URL 的 path 合并——base path 中最后一个 `/` 之后的所有内容都会被替换。末尾斜杠非常重要：`https://example.com/dir/` + `data.csv` 得到 `https://example.com/dir/data.csv`，而 `https://example.com/dir` + `data.csv` 得到 `https://example.com/data.csv`。点分段 (`./` 和 `../`) 会被归一化。
* **相对于 host** (例如 `/test/data.csv`) ：使用 base URL 的 scheme 和 host 进行解析。
* **相对于 scheme** (例如 `//other.com/test/data.csv`) ：使用 base URL 的 scheme 进行解析。
* **仅查询字符串** (例如 `?x=1`) ：附加到完整的 base path，并替换现有的查询字符串或片段。
* **仅片段** (例如 `#frag`) ：附加到 base URL，保留查询字符串，并替换现有片段。
* **空值**：返回不带片段的 base URL。
* **绝对 URL**：保持原样传递；忽略 `url_base`。

**示例**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## 存储设置
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/zh/operations/settings/settings.md#engine_url_skip_empty_files) - 允许在读取时跳过空文件。默认禁用。
* [enable&#95;url&#95;encoding](/zh/operations/settings/settings.md#enable_url_encoding) - 允许启用/禁用 URI 中路径的解码/编码。默认启用。
* [url&#95;base](/zh/operations/settings/settings.md#url_base) - 用于解析传递给 `url` function 的相对 URL 的base URL。

<div id="permissions">
  ## 权限
</div>

`url` function 需要 `CREATE TEMPORARY TABLE` 权限。因此，设置了 [readonly](/zh/operations/settings/permissions-for-queries#readonly) = 1 的用户将无法使用该函数。至少需要 readonly = 2。

<div id="related">
  ## 相关内容
</div>

* [虚拟列](/zh/engines/table-engines/index.md#table_engines-virtual_columns)