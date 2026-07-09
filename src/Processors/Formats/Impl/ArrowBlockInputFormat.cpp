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

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats. ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow's "file mode" format. It is designed for in-memory random access.

## Data types matching {#data-types-matching}

The table below shows the supported data types and how they correspond to ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/sql-reference/data-types/datetime64.md)                                              | `TIMESTAMP`                |
| `STRING`, `BINARY`                      | [String](/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`    |
| `INT64`                                 | [Interval](/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year) | `INT64`         |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [`output_format_arrow_low_cardinality_as_dictionary`](/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) setting that allows to output [LowCardinality](/sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type. Note that there might be unused values in `LowCardinality` dictionary, which can lead to unused values in Arrow `DICTIONARY` during output.

Unsupported Arrow data types:
- `JSON`
- `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

You can insert Arrow data from a file into ClickHouse table using the following command:

```bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

### Selecting data {#selecting-data}

You can select data from a ClickHouse table and save it into some file in the Arrow format using the following command:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
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

## Format settings {#format-settings}
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
