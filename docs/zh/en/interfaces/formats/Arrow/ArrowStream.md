---
alias: []
description: 'ArrowStream 格式文档'
input_format: true
keywords: ['ArrowStream']
output_format: true
slug: /interfaces/formats/ArrowStream
title: 'ArrowStream'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 描述
</div>

`ArrowStream` 是 Apache Arrow 的“流模式”格式，专为内存中的流处理而设计。

<div id="example-usage">
  ## 示例用法
</div>

在下面的示例中，我们使用 [ClickHouse SQL playground](https://sql.clickhouse.com) 中提供的 `forex` 数据集。您可以使用 `clickhouse-client`，通过主机 `sql-clickhouse.clickhouse.com` 和用户 `demo` (无密码) 远程连接到它。`forex` 表位于
`forex` 数据库中，因此我们将其选为默认数据库：

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

`forex` 表用于存储货币汇率。我们可以通过查询 [`system.columns`](/zh/operations/system-tables/columns) 查看它的大小，
以及它在磁盘上的压缩情况：

```sql title="Query"
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
FROM system.columns
WHERE (database = 'forex') AND (table = 'forex')
GROUP BY table
ORDER BY table ASC
```

```response title="Response"
   ┌─table─┬─compressed_size─┬─uncompressed_size─┬───compression_ratio─┐
1. │ forex │ 63.69 GiB       │ 280.48 GiB        │ 0.22708227109363446 │
   └───────┴─────────────────┴───────────────────┴─────────────────────┘
```

与 [`Arrow`](/zh/interfaces/formats/Arrow) 的“文件模式”格式不同，后者必须等整个结果全部生成后才能读取，而 `ArrowStream` 会以记录批次序列的形式传输，消费者可以在其到达时增量读取。这使它非常适合将查询结果直接流式传输到可视化或分析工具中，而无需先将整个数据集物化。

要流式传输结果，请通过 ClickHouse 的 HTTP 接口使用 `POST` 请求发送查询，并将响应作为 Arrow 流读取。我们通过
[`output_format_arrow_compression_method`](/zh/operations/settings/formats#output_format_arrow_compression_method)
设置禁用 Arrow 输出的压缩，以便消费者在接收到批次时可以直接解码。

`ArrowStream` 输出是原始二进制数据，因此我们不会将其打印到终端，而是通过管道将其传给消费者。该流是自描述的 (它自带 schema) ，因此这里我们直接通过管道将其传给
[`clickhouse-local`](/zh/operations/utilities/clickhouse-local)，后者使用 `--input-format ArrowStream` 读取传入批次，并将其作为一张表来查询。
`forex` 表很大，因此我们使用 `WHERE`
谓词和 `LIMIT` 来限制远程查询，以使本示例保持简洁：

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'USD' AND quote = 'CHF'
        ORDER BY datetime ASC
        LIMIT 5
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬────bid─┬────ask─┬────────────────spread─┐
1. │ USD.CHF    │ 2000-05-30 17:23:44.000 │  1.688 │ 1.6885 │ 0.0005000829696655273 │
2. │ USD.CHF    │ 2000-05-30 17:23:46.000 │ 1.6885 │  1.689 │ 0.0004999637603759766 │
3. │ USD.CHF    │ 2000-05-30 17:23:48.000 │ 1.6886 │ 1.6891 │ 0.0005000829696655273 │
4. │ USD.CHF    │ 2000-05-30 17:23:49.000 │ 1.6888 │ 1.6893 │ 0.0004999637603759766 │
5. │ USD.CHF    │ 2000-05-30 17:24:45.000 │  1.689 │ 1.6895 │ 0.0004999637603759766 │
   └────────────┴─────────────────────────┴────────┴────────┴───────────────────────┘
```

同一个流可以被任何支持 Arrow 的客户端以增量方式消费，也就是
按批次逐批读取，而不是将整个结果一次性缓冲。例如，
使用 [Apache Arrow JavaScript library](https://arrow.apache.org/docs/js/)，
`RecordBatchReader` 会在每个记录批次从
服务器流式传输出来后立即产出该批次：

```js
const reader = await RecordBatchReader.from(response);
await reader.open();
for await (const recordBatch of reader) {
    const batchTable = new Table(recordBatch);
    const ipcStream = tableToIPC(batchTable, 'stream');
    const bytes = new Uint8Array(ipcStream);
    table.update(bytes);
}
```

如需查看如何将 ClickHouse 中的流式 `ArrowStream` 数据导入
借助 [Perspective](https://perspective.finos.org/) 实现的实时可视化的完整演练，请参阅
这篇博客文章
[使用 ClickHouse、Apache Arrow 和 Perspective 实现实时流式可视化](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective)。

<div id="format-settings">
  ## 格式设置
</div>

`ArrowStream` 与 [`Arrow`](/zh/interfaces/formats/Arrow) 格式使用相同的格式设置。

| Setting                                                                      | Description                                                                                                                                | Default     |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | 读取 Arrow 输入格式时允许列缺失                                                                                                                        | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | 将 Arrow 列与 CH 列匹配时忽略大小写。                                                                                                                   | `0`         |
| `input_format_arrow_import_nested`                                           | 已废弃，不起任何作用。                                                                                                                                | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | 对 Arrow 格式进行 schema 推断时，跳过类型不受支持的列                                                                                                         | `0`         |
| `input_format_arrow_use_native_reader`                                       | 对 `Arrow` 和 `ArrowStream` 格式使用原生 ClickHouse reader，而不是 Apache Arrow library。设为 `0` 可使用 Apache Arrow library reader。                        | `1`         |
| `output_format_arrow_compression_method`                                     | Arrow 输出格式的压缩方法。支持的编解码器：lz4&#95;frame、zstd、none (未压缩)                                                                                      | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | 将 Date 值写为普通 16 位数字 (读取时为 UInt16) ，而不是转换为 32 位的 Arrow DATE32 类型 (读取时为 Date32) 。                                                            | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | 对 FixedString 列，使用 Arrow FIXED&#95;SIZE&#95;BINARY 类型而不是 Binary。                                                                           | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | 启用将 LowCardinality 类型输出为 Arrow 的 Dictionary 类型                                                                                             | `0`         |
| `output_format_arrow_string_as_string`                                       | 对 String 列，使用 Arrow String 类型而不是 Binary                                                                                                    | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | 将没有对应 Arrow 等价类型的类型 (例如 `BFloat16`、`AggregateFunction`) 作为原始二进制数据输出。如果为 `false`，此类类型会引发异常。此设置同时适用于原生 writer 和 Apache Arrow library writer。 | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | 在 Arrow 格式中始终对字典索引使用 64 位整数                                                                                                                | `0`         |
| `output_format_arrow_use_native_writer`                                      | 对 `Arrow` 和 `ArrowStream` 格式使用原生 ClickHouse writer，而不是 Apache Arrow library。设为 `0` 可使用 Apache Arrow library writer。                        | `1`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | 在 Arrow 格式中对字典索引使用有符号整数                                                                                                                    | `1`         |