---
alias: []
description: 'Avro 格式文档'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

[Apache Avro](https://avro.apache.org/) 是一种面向行的序列化格式，使用二进制编码来实现高效的数据处理。`Avro` 格式支持读取和写入 [Avro 数据文件](https://avro.apache.org/docs/current/specification/#object-container-files)。该格式要求消息为自描述，并嵌入 schema。如果你将 Avro 与 Schema Registry 一起使用，请参阅 [`AvroConfluent`](./AvroConfluent.md) 格式。

<div id="data-type-mapping">
  ## 数据类型映射
</div>

<DataTypeMapping />

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                         | 描述                                                                                          | 默认值     |
| ------------------------------------------ | ------------------------------------------------------------------------------------------- | ------- |
| `input_format_avro_allow_missing_fields`   | 当 schema 中找不到某个 field 时，是否使用默认值而不是报错。                                                       | `0`     |
| `input_format_avro_null_as_default`        | 当向非 Nullable 列插入 `null` 值时，是否使用默认值而不是报错。                                                    | `0`     |
| `output_format_avro_codec`                 | Avro 输出文件的压缩算法。可能的值：`null`、`deflate`、`snappy`、`zstd`。                                       |         |
| `output_format_avro_sync_interval`         | Avro 文件中同步标记的频率 (以字节为单位) 。                                                                  | `16384` |
| `output_format_avro_string_column_pattern` | 用于识别需映射为 Avro String 类型的 `String` 列的正则表达式。默认情况下，ClickHouse 的 `String` 列会写为 Avro `bytes` 类型。 |         |
| `output_format_avro_rows_in_file`          | 每个 Avro 输出文件的最大行数。达到该限制时，会创建一个新文件 (如果存储系统支持文件拆分) 。                                          | `1`     |

<div id="examples">
  ## 示例
</div>

<div id="reading-avro-data">
  ### 读取 Avro 数据
</div>

要将 Avro 文件中的数据读入 ClickHouse 表：

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

摄取的 Avro 文件的根 schema 必须为 `record` 类型。

为了确定表的列与 Avro schema 字段之间的对应关系，ClickHouse 会比较它们的名称。
这种比较区分大小写，未使用的字段会被跳过。

ClickHouse 表列的数据类型可以与插入的 Avro 数据中对应字段的数据类型不同。插入数据时，ClickHouse 会根据上表解释数据类型，然后将数据[转换](/zh/sql-reference/functions/type-conversion-functions#CAST)为相应的列类型。

导入数据时，如果在 schema 中找不到某个字段，并且启用了设置 [`input_format_avro_allow_missing_fields`](/zh/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields)，则会使用默认值，而不会抛出错误。

<div id="writing-avro-data">
  ### 写入 Avro 数据
</div>

要将 ClickHouse 表中的数据写入 Avro 文件，请按以下步骤操作：

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

列名必须满足以下条件：

* 以 `[A-Za-z_]` 开头
* 后续字符只能是 `[A-Za-z0-9_]`

Avro 文件的输出压缩和同步间隔分别可通过 [`output_format_avro_codec`](/zh/operations/settings/settings-formats.md/#output_format_avro_codec) 和 [`output_format_avro_sync_interval`](/zh/operations/settings/settings-formats.md/#output_format_avro_sync_interval) 设置进行配置。

<div id="inferring-the-avro-schema">
  ### 推断 Avro schema
</div>

使用 ClickHouse [`DESCRIBE`](/zh/sql-reference/statements/describe-table) 函数，你可以像下面的示例那样快速查看推断出的 Avro 文件格式。
此示例包含 ClickHouse S3 公共 bucket 中一个可公开访问的 Avro 文件的 URL：

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```