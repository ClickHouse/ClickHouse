---
description: '提供对亚马逊 S3 中 Apache Hudi 表的只读类表接口。'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

提供对亚马逊 S3 中 Apache [Hudi](https://hudi.apache.org/) 表的只读类表接口。

<div id="syntax">
  ## 语法
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## 参数
</div>

| Argument                                     | Description                                                                                                                                                                                                               |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | 指向 S3 中现有 Hudi 表路径的存储桶 URL。                                                                                                                                                                                               |
| `aws_access_key_id`, `aws_secret_access_key` | AWS 账户用户的长期凭据。您可以使用它们对请求进行身份验证。这些参数是可选的。如果未指定凭据，则会使用 ClickHouse 配置中的凭据。更多信息，请参见 [使用 S3 进行数据存储](/zh/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)、[AWS](https://aws.amazon.com/)。           |
| `format`                                     | 文件的 [格式](/zh/interfaces/formats)。                                                                                                                                                                                            |
| `structure`                                  | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                                   |
| `compression`                                | 该参数为可选。支持的值包括：`none`、`gzip/gz`、`brotli/br`、`xz/LZMA`、`zstd/zst`。默认会根据文件扩展名自动检测压缩方式。                                                                                                                                       |
| `extra_credentials`                          | 该参数为可选。用于传递 `role_arn`，以便在 ClickHouse Cloud 中实现基于角色的访问。配置步骤请参见 [Secure S3](/zh/cloud/data-sources/secure-s3) 或 [Using S3 for Data Storage](/zh/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)。 |

<div id="returned_value">
  ## 返回值
</div>

返回一个具有指定结构的表，用于读取 S3 中指定 Hudi 表的数据。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (字节) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

<div id="related">
  ## 相关内容
</div>

* [Hudi 引擎](/zh/engines/table-engines/integrations/hudi.md)
* [Hudi cluster 表函数](/zh/sql-reference/table-functions/hudiCluster.md)