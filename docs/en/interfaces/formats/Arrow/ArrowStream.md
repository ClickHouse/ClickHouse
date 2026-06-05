---
alias: []
description: 'Documentation for the ArrowStream format'
input_format: true
keywords: ['ArrowStream']
output_format: true
slug: /interfaces/formats/ArrowStream
title: 'ArrowStream'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`ArrowStream` is Apache Arrow's "stream mode" format. It is designed for in-memory stream processing.

## Example usage {#example-usage}

In the example below we use the `forex` dataset which is available in the
[ClickHouse SQL playground](https://sql.clickhouse.com). You can connect to it
remotely with `clickhouse-client` using the host `sql-clickhouse.clickhouse.com`
and the user `demo` (which has no password). The `forex` table lives in the
`forex` database, so we select it as the default database:

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

The `forex` table stores currency exchange rates. We can inspect its size and
how well it compresses on disk by querying [`system.columns`](/operations/system-tables/columns):

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

Unlike the [`Arrow`](/interfaces/formats/Arrow) "file mode" format, which
requires the whole result before it can be read, `ArrowStream` is delivered as a
sequence of record batches that a consumer can read incrementally as they
arrive. This makes it well suited to streaming a query result straight into a
visualization or analytics tool without first materializing the entire dataset.

To stream the result, send the query over ClickHouse's HTTP interface with a
`POST` request and read the response as an Arrow stream. We disable compression
of the Arrow output via the
[`output_format_arrow_compression_method`](/operations/settings/formats#output_format_arrow_compression_method)
setting so that consumers can decode batches directly as they are received.

The `ArrowStream` output is raw binary, so rather than printing it to the
terminal we pipe it into a consumer. The stream is self-describing (it carries
its own schema), so here we pipe it straight into
[`clickhouse-local`](/operations/utilities/clickhouse-local), which reads the
incoming batches with `--input-format ArrowStream` and queries them as a table:

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
        ORDER BY datetime ASC
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC LIMIT 5 FORMAT PrettyCompact"
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

The same stream can be consumed incrementally by any Arrow-aware client, which
reads it batch-by-batch rather than buffering the result in full. For example,
using the [Apache Arrow JavaScript library](https://arrow.apache.org/docs/js/), a
`RecordBatchReader` yields each record batch as soon as it is streamed from the
server:

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

For a full walkthrough of streaming `ArrowStream` data from ClickHouse into a
real-time visualization with [Perspective](https://perspective.finos.org/), see
the blog post
[Streaming real-time visualizations with ClickHouse, Apache Arrow and Perspective](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective).

## Format settings {#format-settings}

`ArrowStream` shares the same format settings as the [`Arrow`](/interfaces/formats/Arrow) format.

| Setting                                                                      | Description                                                                                                                                | Default     |
|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_arrow_allow_missing_columns`                                   | Allow missing columns while reading Arrow input formats                                                                                    | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignore case when matching Arrow columns with CH columns.                                                                                   | `0`         |
| `input_format_arrow_import_nested`                                           | Obsolete setting, does nothing.                                                                                                            | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Arrow                                                                | `0`         |
| `output_format_arrow_compression_method`                                     | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)                                         | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | Write Date values as plain 16-bit numbers (read back as UInt16), instead of converting to a 32-bit Arrow DATE32 type (read back as Date32). | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                                                                | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Enable output LowCardinality type as Dictionary Arrow type                                                                                 | `0`         |
| `output_format_arrow_string_as_string`                                       | Use Arrow String type instead of Binary for String columns                                                                                 | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | Output types having no conversion as raw binary data. If false - such types would raise UNKNOWN_TYPE exception.                             | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Always use 64 bit integers for dictionary indexes in Arrow format                                                                          | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Use signed integers for dictionary indexes in Arrow format                                                                                 | `1`         |
