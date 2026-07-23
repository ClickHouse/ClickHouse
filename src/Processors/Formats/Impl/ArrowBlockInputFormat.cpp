#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <Processors/Port.h>
#include <optional>

#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/NetUtils.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/result.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

ArrowBlockInputFormat::ArrowBlockInputFormat(ReadBuffer & in_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IInputFormat(header_, &in_)
    , stream(stream_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

Chunk ArrowBlockInputFormat::read()
{
    Chunk res;
    block_missing_values.clear();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_result;
    size_t batch_start = getDataOffsetMaybeCompressed(*in);
    if (stream)
    {
        if (!stream_reader)
            prepareReader();

        if (is_stopped)
            return {};

        batch_result = stream_reader->Next();
        if (batch_result.ok() && !(*batch_result))
        {
            /// Make sure we try to read past the end to fully drain the ReadBuffer (e.g. read
            /// compression frame footer or HTTP chunked encoding's final empty chunk).
            /// This is needed for HTTP keepalive.
            in->eof();

            return res;
        }

        if (need_only_count && batch_result.ok())
            return getChunkForCount((*batch_result)->num_rows());
    }
    else
    {
        if (!file_reader)
            prepareReader();

        if (is_stopped)
            return {};

        if (record_batch_current >= record_batch_total)
        {
            in->eof();
            return res;
        }

        if (need_only_count)
        {
            auto rows = file_reader->RecordBatchCountRows(record_batch_current++);
            if (!rows.ok())
                throwFromArrowStatus(rows.status(), ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of Arrow data");
            return getChunkForCount(*rows);
        }

        batch_result = file_reader->ReadRecordBatch(record_batch_current);
    }

    if (!batch_result.ok())
        throwFromArrowStatus(batch_result.status(), ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of Arrow data");

    /// Validate validity bitmaps before building the table: Table::FromRecordBatches computes
    /// each column's null_count, and Arrow derives an unknown FieldNode null_count by scanning
    /// the bitmap over the declared length, which reads out of bounds on a truncated bitmap.
    ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(**batch_result);

    auto table_result = arrow::Table::FromRecordBatches({*batch_result});
    if (!table_result.ok())
        throwFromArrowStatus(table_result.status(), ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of Arrow data");

    ++record_batch_current;

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    auto schema_metadata = stream ? stream_reader->schema()->metadata() : file_reader->schema()->metadata();
    res = arrow_column_to_ch_column->arrowTableToCHChunk(*table_result, (*table_result)->num_rows(), schema_metadata, block_missing_values_ptr);

    /// There is no easy way to get original record batch size from Arrow metadata.
    /// Let's just use the number of bytes read from read buffer.
    auto batch_end = getDataOffsetMaybeCompressed(*in);
    if (batch_end > batch_start)
        approx_bytes_read_for_chunk = batch_end - batch_start;
    return res;
}

void ArrowBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    if (stream)
        stream_reader.reset();
    else
        file_reader.reset();
    record_batch_current = 0;
    block_missing_values.clear();
}

const BlockMissingValues * ArrowBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

static std::shared_ptr<arrow::RecordBatchReader> createStreamReader(ReadBuffer & in)
{
    /// Validate the stream before passing it to the Arrow library.
    /// Arrow IPC streaming format interprets the first 4 bytes as either:
    ///   - a continuation token (0xFFFFFFFF for modern format, >= v0.15.0), or
    ///   - the metadata length directly (legacy format, < v0.15.0).
    /// If the data is not actually Arrow IPC (e.g., JSON, CSV), these bytes get
    /// interpreted as a huge metadata length, causing Arrow to allocate hundreds
    /// of megabytes of memory before discovering the data is invalid.
    /// For example, JSON starting with "{\n  " is interpreted as a ~514 MiB metadata length.
    if (in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");

    constexpr int32_t kIpcContinuationToken = -1; /// 0xFFFFFFFF
    /// Even a schema with thousands of columns and extensive metadata
    /// would have a Flatbuffer well under a megabyte. 256 MiB is an extremely
    /// conservative upper bound — any metadata length above this is certainly
    /// not valid Arrow IPC data and is the result of misinterpreting random bytes.
    constexpr int32_t max_reasonable_metadata_length = 256 * 1024 * 1024;

    if (in.available() >= sizeof(int32_t))
    {
        int32_t first_int = 0;
        memcpy(&first_int, in.position(), sizeof(int32_t));
        /// Arrow IPC uses little-endian byte order on the wire.
        first_int = DB::fromLittleEndian(first_int);

        /// In the modern format, the first 4 bytes must be the continuation token 0xFFFFFFFF.
        /// In the legacy format, the first 4 bytes are the metadata length (a positive int32).
        /// Anything else (zero is handled as EOS by Arrow, negative other than -1 is an error)
        /// or a metadata length that is unreasonably large indicates this is not Arrow IPC data.
        if (first_int != kIpcContinuationToken && (first_int <= 0 || first_int > max_reasonable_metadata_length))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow IPC stream");
    }

    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.memory_pool = ArrowMemoryPool::instance();
    auto stream_reader_status = arrow::ipc::RecordBatchStreamReader::Open(std::make_unique<ArrowInputStreamFromReadBuffer>(in), options);
    if (!stream_reader_status.ok())
        throwFromArrowStatus(stream_reader_status.status(), ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table");
    return *stream_reader_status;
}

static std::shared_ptr<arrow::ipc::RecordBatchFileReader> createFileReader(
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Arrow", ARROW_MAGIC_BYTES);
    if (is_stopped)
        return nullptr;

    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.memory_pool = ArrowMemoryPool::instance();
    auto file_reader_status = arrow::ipc::RecordBatchFileReader::Open(arrow_file, options);
    if (!file_reader_status.ok())
        throwFromArrowStatus(file_reader_status.status(), ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table");
    return *file_reader_status;
}


void ArrowBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    if (stream)
    {
        stream_reader = createStreamReader(*in);
        schema = stream_reader->schema();
    }
    else
    {
        file_reader = createFileReader(*in, format_settings, is_stopped);
        if (!file_reader)
            return;
        schema = file_reader->schema();
    }

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Arrow",
        format_settings,
        std::nullopt,
        std::nullopt,
        format_settings.arrow.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser,
        format_settings.arrow.case_insensitive_column_matching,
        stream);

    if (stream)
        record_batch_total = -1;
    else
        record_batch_total = file_reader->num_record_batches();

    record_batch_current = 0;
}

ArrowSchemaReader::ArrowSchemaReader(ReadBuffer & in_, bool stream_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), stream(stream_), format_settings(format_settings_)
{
}

void ArrowSchemaReader::initializeIfNeeded()
{
    if (file_reader || stream_reader)
        return;

    if (stream)
        stream_reader = createStreamReader(in);
    else
    {
        std::atomic<int> is_stopped = 0;
        file_reader = createFileReader(in, format_settings, is_stopped);
    }
}

NamesAndTypesList ArrowSchemaReader::readSchema()
{
    initializeIfNeeded();

    std::shared_ptr<arrow::Schema> schema;

    if (stream)
        schema = stream_reader->schema();
    else
        schema = file_reader->schema();

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema,
        schema->metadata(),
        stream ? "ArrowStream" : "Arrow",
        format_settings,
        format_settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
        format_settings.schema_inference_make_columns_nullable != 0,
        false,
        format_settings.parquet.allow_geoparquet_parser);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> ArrowSchemaReader::readNumberOrRows()
{
    if (stream)
        return std::nullopt;

    auto rows = file_reader->CountRows();
    if (!rows.ok())
        throwFromArrowStatus(rows.status(), ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of Arrow data");

    return *rows;
}

void registerInputFormatArrow(FormatFactory & factory);
void registerInputFormatArrow(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Arrow",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & /* params */,
           const FormatSettings & format_settings) -> InputFormatPtr
        {
            auto header = std::make_shared<const Block>(sample);
            if (format_settings.arrow.input_use_native_reader)
                return std::make_shared<ArrowIPCBlockInputFormat>(buf, header, false, format_settings);
            return std::make_shared<ArrowBlockInputFormat>(buf, header, false, format_settings);
        });
    factory.markFormatSupportsSubsetOfColumns("Arrow");
    factory.registerInputFormat(
        "ArrowStream",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & /* params */,
           const FormatSettings & format_settings) -> InputFormatPtr
        {
            auto header = std::make_shared<const Block>(sample);
            if (format_settings.arrow.input_use_native_reader)
                return std::make_shared<ArrowIPCBlockInputFormat>(buf, header, true, format_settings);
            return std::make_shared<ArrowBlockInputFormat>(buf, header, true, format_settings);
        });
    factory.markFormatSupportsSubsetOfColumns("ArrowStream");

    factory.setDocumentation("Arrow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats.
ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow's "file mode" format, designed for in-memory random access.

## Data types matching {#data-types-matching}

The table below shows the supported data types and how they correspond to ClickHouse [data types](/reference/data-types/index) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/reference/data-types/boolean)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/reference/data-types/int-uint)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/reference/data-types/int-uint)/[Enum8](/reference/data-types/enum)   | `INT8`                     |
| `UINT16`                                | [UInt16](/reference/data-types/int-uint)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/reference/data-types/int-uint)/[Enum16](/reference/data-types/enum) | `INT16`                    |
| `UINT32`                                | [UInt32](/reference/data-types/int-uint)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/reference/data-types/int-uint)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/reference/data-types/int-uint)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/reference/data-types/int-uint)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/reference/data-types/float)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/reference/data-types/float)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/reference/data-types/date32)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/reference/data-types/datetime)                                                  | `UINT32`                   |
| `TIMESTAMP`                             | [DateTime64](/reference/data-types/datetime64)                                              | `TIMESTAMP`                |
| `TIME32`, `TIME64`                      | [Time64](/reference/data-types/time64)                                              | `TIME32`, `TIME64`                |
| `STRING`, `BINARY`                      | [String](/reference/data-types/string)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/reference/data-types/fixedstring)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/reference/data-types/decimal)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/reference/data-types/decimal)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/reference/data-types/array)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/reference/data-types/tuple)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/reference/data-types/map)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/reference/data-types/ipv4)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/reference/data-types/ipv6)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint)                             | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/reference/data-types/special-data-types/interval) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`    |
| `INT64`                                 | [Interval](/reference/data-types/special-data-types/interval) (Minute/Hour/Day/Week/Month/Quarter/Year) | `INT64`         |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [`output_format_arrow_low_cardinality_as_dictionary`](/reference/settings/formats#output_format_arrow_low_cardinality_as_dictionary) setting that allows to output [LowCardinality](/reference/data-types/lowcardinality) type as a `DICTIONARY` type. Note that there might be unused values in `LowCardinality` dictionary, which can lead to unused values in Arrow `DICTIONARY` during output.

Unsupported Arrow data types: 
- `JSON`
- `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

In the example below we use the `forex` dataset available in the
[ClickHouse SQL playground](https://sql.clickhouse.com).

### Selecting data {#selecting-data}

We select one day of `EUR/USD` exchange rates from the playground and save it
into a local `forex_eurusd.arrow` file. We query the playground over the HTTP
interface, where the host is `sql-clickhouse.clickhouse.com` and the user is
`demo` (which has no password):

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
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

### Reading the file back {#reading-data}

We can now read the local Arrow file back with
[`clickhouse-local`](/concepts/features/tools-and-utilities/clickhouse-local) using the
[`file`](/reference/functions/table-functions/file) table function. The file is
self-describing, so the `Arrow` format infers the schema automatically:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

### Inserting data {#inserting-data}

To load an Arrow file into a ClickHouse table, pipe it into `clickhouse-client`
with `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

## Format settings {#format-settings}

| Setting                                                                                                                  | Description                                                                                        | Default      |
|--------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|--------------|
| `input_format_arrow_allow_missing_columns`                                                                               | Allow missing columns while reading Arrow input formats                                            | `1`          |
| `input_format_arrow_case_insensitive_column_matching`                                                                    | Ignore case when matching Arrow columns with CH columns.                                           | `0`          |
| `input_format_arrow_import_nested`                                                                                       | Obsolete setting, does nothing.                                                                    | `0`          |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`                                             | Skip columns with unsupported types while schema inference for format Arrow                        | `0`          |
| `input_format_arrow_use_native_reader`                                                                                   | Use the native ClickHouse reader for the `Arrow` and `ArrowStream` formats instead of the Apache Arrow library. Set to `0` to use the Apache Arrow library reader. | `1`          |
| `output_format_arrow_compression_method`                                                                                 | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed) | `lz4_frame`  |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                                                                   | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                        | `1`          |
| `output_format_arrow_low_cardinality_as_dictionary`                                                                      | Enable output LowCardinality type as Dictionary Arrow type                                         | `0`          |
| `output_format_arrow_string_as_string`                                                                                   | Use Arrow String type instead of Binary for String columns                                         | `1`          |
| `output_format_arrow_unsupported_types_as_binary`                                                                        | Output a type that has no Arrow equivalent (e.g. `BFloat16`, `AggregateFunction`) as raw binary data. If false, such a type raises an exception. Applies to both the native and the Apache Arrow library writer. | `1`          |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                                                                  | Always use 64 bit integers for dictionary indexes in Arrow format                                  | `0`          |
| `output_format_arrow_use_native_writer`                                                                                  | Use the native ClickHouse writer for the `Arrow` and `ArrowStream` formats instead of the Apache Arrow library. Set to `0` to use the Apache Arrow library writer. | `1`          |
| `output_format_arrow_use_signed_indexes_for_dictionary`                                                                  | Use signed integers for dictionary indexes in Arrow format                                         | `1`          |
)DOCS_MD"});

    factory.setDocumentation("ArrowStream", Documentation{
        .description = R"DOCS_MD(
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
how well it compresses on disk by querying [`system.columns`](/reference/system-tables/columns):

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

Unlike the [`Arrow`](/reference/formats/Arrow/Arrow) "file mode" format, which
requires the whole result before it can be read, `ArrowStream` is delivered as a
sequence of record batches that a consumer can read incrementally as they
arrive. This makes it well suited to streaming a query result straight into a
visualization or analytics tool without first materializing the entire dataset.

To stream the result, send the query over ClickHouse's HTTP interface with a
`POST` request and read the response as an Arrow stream. We disable compression
of the Arrow output via the
[`output_format_arrow_compression_method`](/reference/settings/formats#output_format_arrow_compression_method)
setting so that consumers can decode batches directly as they are received.

The `ArrowStream` output is raw binary, so rather than printing it to the
terminal we pipe it into a consumer. The stream is self-describing (it carries
its own schema), so here we pipe it straight into
[`clickhouse-local`](/concepts/features/tools-and-utilities/clickhouse-local), which reads the
incoming batches with `--input-format ArrowStream` and queries them as a table.
The `forex` table is large, so we bound the remote query with a `WHERE`
predicate and a `LIMIT` to keep this example small:

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

`ArrowStream` shares the same format settings as the [`Arrow`](/reference/formats/Arrow/Arrow) format.

| Setting                                                                      | Description                                                                                                                                | Default     |
|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_arrow_allow_missing_columns`                                   | Allow missing columns while reading Arrow input formats                                                                                    | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignore case when matching Arrow columns with CH columns.                                                                                   | `0`         |
| `input_format_arrow_import_nested`                                           | Obsolete setting, does nothing.                                                                                                            | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Arrow                                                                | `0`         |
| `input_format_arrow_use_native_reader`                                       | Use the native ClickHouse reader for the `Arrow` and `ArrowStream` formats instead of the Apache Arrow library. Set to `0` to use the Apache Arrow library reader. | `1`         |
| `output_format_arrow_compression_method`                                     | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)                                         | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | Write Date values as plain 16-bit numbers (read back as UInt16), instead of converting to a 32-bit Arrow DATE32 type (read back as Date32). | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                                                                | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Enable output LowCardinality type as Dictionary Arrow type                                                                                 | `0`         |
| `output_format_arrow_string_as_string`                                       | Use Arrow String type instead of Binary for String columns                                                                                 | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | Output a type that has no Arrow equivalent (e.g. `BFloat16`, `AggregateFunction`) as raw binary data. If false, such a type raises an exception. Applies to both the native and the Apache Arrow library writer. | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Always use 64 bit integers for dictionary indexes in Arrow format                                                                          | `0`         |
| `output_format_arrow_use_native_writer`                                      | Use the native ClickHouse writer for the `Arrow` and `ArrowStream` formats instead of the Apache Arrow library. Set to `0` to use the Apache Arrow library writer. | `1`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Use signed integers for dictionary indexes in Arrow format                                                                                 | `1`         |
)DOCS_MD"});
}

void registerArrowSchemaReader(FormatFactory & factory);
void registerArrowSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Arrow",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            if (settings.arrow.input_use_native_reader)
                return std::make_shared<ArrowIPCSchemaReader>(buf, false, settings);
            return std::make_shared<ArrowSchemaReader>(buf, false, settings);
        });

    factory.registerAdditionalInfoForSchemaCacheGetter("Arrow", [](const FormatSettings & settings)
    {
        return fmt::format(
            "schema_inference_make_columns_nullable={};schema_inference_allow_nullable_tuple_type={};"
            "use_native_reader={};skip_columns_with_unsupported_types={};allow_geoparquet_parser={}",
            settings.schema_inference_make_columns_nullable,
            settings.schema_inference_allow_nullable_tuple_type,
            settings.arrow.input_use_native_reader,
            settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
            settings.parquet.allow_geoparquet_parser);
    });
    factory.registerSchemaReader(
        "ArrowStream",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            if (settings.arrow.input_use_native_reader)
                return std::make_shared<ArrowIPCSchemaReader>(buf, true, settings);
            return std::make_shared<ArrowSchemaReader>(buf, true, settings);
        });

    factory.registerAdditionalInfoForSchemaCacheGetter("ArrowStream", [](const FormatSettings & settings)
    {
        return fmt::format(
            "schema_inference_make_columns_nullable={};schema_inference_allow_nullable_tuple_type={};"
            "use_native_reader={};skip_columns_with_unsupported_types={};allow_geoparquet_parser={}",
            settings.schema_inference_make_columns_nullable,
            settings.schema_inference_allow_nullable_tuple_type,
            settings.arrow.input_use_native_reader,
            settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
            settings.parquet.allow_geoparquet_parser);
    });
}

}
#else

namespace DB
{
class FormatFactory;
void registerInputFormatArrow(FormatFactory &);
void registerArrowSchemaReader(FormatFactory &);
void registerInputFormatArrow(FormatFactory &)
{
}

void registerArrowSchemaReader(FormatFactory &) {}
}

#endif
