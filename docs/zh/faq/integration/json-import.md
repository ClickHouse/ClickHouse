---
slug: /zh/faq/integration/json-import
title: 如何将 JSON 导入到 ClickHouse？
toc_hidden: true
sidebar_position: 11
---

# 如何将 JSON 导入到 ClickHouse？ {#how-to-import-json-into-clickhouse}

ClickHouse 支持多种[输入和输出的数据格式](../../interfaces/formats.md)。其中包括多种 JSON 变体，但最常用于数据导入的是 [JSONEachRow](../../interfaces/formats.md#jsoneachrow)。它期望每行一个 JSON 对象，每个对象由一个新行分隔。

## 示例 {#examples}

使用 [HTTP 接口](../../interfaces/http.md)：

``` bash
$ echo '{"foo":"bar"}' | curl 'http://localhost:8123/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-
```

使用 [CLI接口](../../interfaces/cli.md):

``` bash
$ echo '{"foo":"bar"}'  | clickhouse-client --query="INSERT INTO test FORMAT JSONEachRow"
```

除了手动插入数据外，您可能会考虑使用 [客户端库](../../interfaces/index.md) 之一。

## 实用设置 {#useful-settings}

-   `input_format_skip_unknown_fields` 允许插入 JSON，即使存在表格架构中未出现的额外字段（通过丢弃它们）。
-   `input_format_import_nested_json` 允许将嵌套 JSON 对象插入到 [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) 类型的列中。

:::note
对于 HTTP 接口，设置作为 `GET` 参数指定；对于 `CLI` 接口，则作为前缀为 -- 的附加命令行参数。
:::