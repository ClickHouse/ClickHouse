#include <Processors/Formats/Impl/VortexBlockInputFormat.h>

#if USE_VORTEX

#include <Core/Defines.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>

#include <vortex_ffi.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}

static constexpr auto VORTEX_MAGIC_BYTES = "VTXF";

/// The context of the read callback passed to the Rust vortex library. The library calls the
/// callback only from inside FFI calls, on the calling thread, so no synchronization is needed.
struct VortexReadContext
{
    arrow::io::RandomAccessFile * file = nullptr;
    size_t bytes_read = 0;
    std::exception_ptr exception;
};

extern "C" int32_t vortexFFIReadCallback(void * context, uint64_t offset, uint64_t length, uint8_t * out);
extern "C" int32_t vortexFFIReadCallback(void * context, uint64_t offset, uint64_t length, uint8_t * out)
{
    auto * ctx = static_cast<VortexReadContext *>(context);
    try
    {
        auto result = ctx->file->ReadAt(static_cast<int64_t>(offset), static_cast<int64_t>(length), out);
        if (!result.ok())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Vortex file: {}", result.status().ToString());
        if (*result != static_cast<int64_t>(length))
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Unexpected end of Vortex file: read {} bytes instead of {} at offset {}",
                *result, length, offset);
        ctx->bytes_read += length;
        return 0;
    }
    catch (...)
    {
        ctx->exception = std::current_exception();
        return 1;
    }
}

/// Throws an exception for a failed FFI call: either the exception thrown by the IO callback,
/// or an exception with the error message returned by the library.
[[noreturn]] static void throwVortexError(char * error, const std::exception_ptr & callback_exception, int code = ErrorCodes::INCORRECT_DATA)
{
    String message = error ? String(error) : "unknown error";
    if (error)
        vortex_ffi_free_string(error);
    if (callback_exception)
        std::rethrow_exception(callback_exception);
    throw Exception(code, "Error while reading Vortex file: {}", message);
}

static void throwFromArrowStatusIfFailed(const arrow::Status & status)
{
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Error while reading Vortex file: {}", status.ToString());
}

/// Opens a Vortex file over a ClickHouse read buffer and reads its Arrow schema.
static VortexFFIReader * openVortexReader(
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped,
    std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    VortexReadContext & read_context,
    std::shared_ptr<arrow::Schema> & file_schema)
{
    arrow_file = asArrowFile(in, format_settings, is_stopped, "Vortex", VORTEX_MAGIC_BYTES);
    if (is_stopped)
        return nullptr;

    auto file_size = arrow_file->GetSize();
    throwFromArrowStatusIfFailed(file_size.status());

    read_context.file = arrow_file.get();

    char * error = nullptr;
    auto * reader = vortex_ffi_reader_open(&read_context, vortexFFIReadCallback, static_cast<uint64_t>(*file_size), &error);
    if (!reader)
        throwVortexError(error, read_context.exception);

    ArrowSchema c_schema{};
    if (vortex_ffi_reader_schema(reader, &c_schema, &error) != 0)
    {
        vortex_ffi_reader_free(reader);
        throwVortexError(error, read_context.exception);
    }

    auto schema = arrow::ImportSchema(&c_schema);
    if (!schema.ok())
    {
        vortex_ffi_reader_free(reader);
        throwFromArrowStatusIfFailed(schema.status());
    }
    file_schema = *schema;

    return reader;
}

VortexBlockInputFormat::VortexBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(header_, &in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

VortexBlockInputFormat::~VortexBlockInputFormat()
{
    closeReader();
}

void VortexBlockInputFormat::closeReader()
{
    if (scanner)
    {
        vortex_ffi_scanner_free(scanner);
        scanner = nullptr;
    }
    if (reader)
    {
        vortex_ffi_reader_free(reader);
        reader = nullptr;
    }
    file_schema.reset();
    arrow_file.reset();
    read_context.reset();
}

void VortexBlockInputFormat::prepareReader()
{
    read_context = std::make_unique<VortexReadContext>();
    reader = openVortexReader(*in, format_settings, is_stopped, arrow_file, *read_context, file_schema);
    if (!reader)
        return;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Vortex",
        format_settings,
        std::nullopt,
        std::nullopt,
        /* allow_missing_columns */ true,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser);

    if (need_only_count)
        return;

    /// Read only the columns requested in the header and present in the file. The requested
    /// columns that are not present in the file are filled with default values. If none of the
    /// requested columns is present (or no columns are requested at all), no scanner is created,
    /// and only the number of rows is used.
    ///
    /// A header column can also address a subcolumn of a top-level field: a `Nested`/struct
    /// subcolumn is requested as `name.sub`, and `ArrowColumnToCHColumn` can extract it only if
    /// the whole top-level field `name` was scanned. Keep `Nested::extractTableName` of every
    /// header column for that reason, the same way the `ArrowIPC` reader does — otherwise such a
    /// column would be silently filled with default values.
    std::vector<std::string> column_names;
    std::vector<const char *> column_name_pointers;
    std::unordered_set<std::string> added_column_names;
    auto add_column_name = [&](const std::string & name)
    {
        if (file_schema->GetFieldByName(name) && added_column_names.emplace(name).second)
            column_names.push_back(name);
    };
    for (const auto & column : getPort().getHeader())
    {
        add_column_name(column.name);
        add_column_name(Nested::extractTableName(column.name));
    }
    for (const auto & name : column_names)
        column_name_pointers.push_back(name.c_str());

    if (column_names.empty())
    {
        pending_rows_without_columns = vortex_ffi_reader_row_count(reader);
        return;
    }

    char * error = nullptr;
    scanner = vortex_ffi_scanner_create(reader, column_name_pointers.data(), column_name_pointers.size(), &error);
    if (!scanner)
        throwVortexError(error, read_context->exception);
}

Chunk VortexBlockInputFormat::readWithoutColumns()
{
    if (!pending_rows_without_columns)
        return {};

    size_t num_rows = std::min<UInt64>(pending_rows_without_columns, DEFAULT_BLOCK_SIZE);
    pending_rows_without_columns -= num_rows;

    auto batch = arrow::RecordBatch::Make(arrow::schema(arrow::FieldVector{}), num_rows, arrow::ArrayVector{});
    auto table = arrow::Table::FromRecordBatches({batch});
    throwFromArrowStatusIfFailed(table.status());

    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    return arrow_column_to_ch_column->arrowTableToCHChunk(*table, num_rows, nullptr, block_missing_values_ptr);
}

Chunk VortexBlockInputFormat::read()
{
    block_missing_values.clear();

    if (!reader && !count_returned)
        prepareReader();

    if (is_stopped)
        return {};

    if (need_only_count)
    {
        if (count_returned)
            return {};
        count_returned = true;
        return getChunkForCount(vortex_ffi_reader_row_count(reader));
    }

    if (!scanner)
        return readWithoutColumns();

    char * error = nullptr;
    ArrowArray c_array{};
    ArrowSchema c_schema{};
    int32_t result = vortex_ffi_scanner_next(scanner, &c_array, &c_schema, &error);
    if (result < 0)
        throwVortexError(error, read_context->exception);
    if (result == 0)
        return {};

    auto batch = arrow::ImportRecordBatch(&c_array, &c_schema);
    throwFromArrowStatusIfFailed(batch.status());

    ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(**batch);

    auto table = arrow::Table::FromRecordBatches({*batch});
    throwFromArrowStatusIfFailed(table.status());

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    Chunk res = arrow_column_to_ch_column->arrowTableToCHChunk(*table, (*table)->num_rows(), nullptr, block_missing_values_ptr);

    approx_bytes_read_for_chunk = read_context->bytes_read - previous_approx_bytes_read;
    previous_approx_bytes_read = read_context->bytes_read;
    return res;
}

void VortexBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    closeReader();
    arrow_column_to_ch_column.reset();
    pending_rows_without_columns = 0;
    count_returned = false;
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
    previous_approx_bytes_read = 0;
}

const BlockMissingValues * VortexBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

VortexSchemaReader::VortexSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

VortexSchemaReader::~VortexSchemaReader()
{
    if (reader)
        vortex_ffi_reader_free(reader);
}

void VortexSchemaReader::initializeIfNeeded()
{
    if (reader)
        return;

    read_context = std::make_unique<VortexReadContext>();
    reader = openVortexReader(in, format_settings, is_stopped, arrow_file, *read_context, file_schema);
}

NamesAndTypesList VortexSchemaReader::readSchema()
{
    initializeIfNeeded();

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *file_schema,
        nullptr,
        "Vortex",
        format_settings,
        /* skip_columns_with_unsupported_types */ false,
        /* allow_arrow_null_type */ true,
        format_settings.schema_inference_make_columns_nullable != 0,
        /* case_insensitive_matching */ false,
        format_settings.parquet.allow_geoparquet_parser);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> VortexSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return vortex_ffi_reader_row_count(reader);
}

void registerInputFormatVortex(FormatFactory & factory);
void registerInputFormatVortex(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "Vortex",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & /* read_settings */,
           bool /* is_remote_fs */,
           FormatParserSharedResourcesPtr /* parser_shared_resources */,
           FormatFilterInfoPtr /* format_filter_info */) -> InputFormatPtr
        {
            return std::make_shared<VortexBlockInputFormat>(buf, std::make_shared<const Block>(sample), settings);
        });
    factory.markFormatSupportsSubsetOfColumns("Vortex");

    factory.setDocumentation("Vortex", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Vortex](https://vortex.dev/) is an extensible columnar file format for compressed Apache Arrow-compatible data,
designed for fast scans and random access. ClickHouse supports reading and writing Vortex files.

## Data types matching {#data-types-matching-vortex}

The table below shows the Vortex data types and the corresponding ClickHouse [data types](/reference/data-types/index)
in `INSERT` and `SELECT` queries.

| Vortex data type (`INSERT`)         | ClickHouse data type                                          | Vortex data type (`SELECT`) |
|-------------------------------------|---------------------------------------------------------------|-----------------------------|
| `Bool`                              | [Bool](/reference/data-types/boolean)                         | `Bool`                      |
| `I8`, `U8`                          | [Int8/UInt8](/reference/data-types/int-uint)                  | `I8`, `U8`                  |
| `I16`, `U16`                        | [Int16/UInt16](/reference/data-types/int-uint)                | `I16`, `U16`                |
| `I32`, `U32`                        | [Int32/UInt32](/reference/data-types/int-uint)                | `I32`, `U32`                |
| `I64`, `U64`                        | [Int64/UInt64](/reference/data-types/int-uint)                | `I64`, `U64`                |
| `F32`                               | [Float32](/reference/data-types/float)                        | `F32`                       |
| `F64`                               | [Float64](/reference/data-types/float)                        | `F64`                       |
| `Utf8`, `Binary`                    | [String](/reference/data-types/string)                        | `Binary`                    |
| `Binary`                            | [FixedString](/reference/data-types/fixedstring)              | `Binary`                    |
| `Decimal`                           | [Decimal](/reference/data-types/decimal)                      | `Decimal`                   |
| `vortex.date`                       | [Date32](/reference/data-types/date32)                        | `vortex.date`               |
| `vortex.timestamp`                  | [DateTime](/reference/data-types/datetime)/[DateTime64](/reference/data-types/datetime64) | `vortex.timestamp` |
| `vortex.time`                       | [Time64](/reference/data-types/time64)                        | `vortex.time`               |
| `List`                              | [Array](/reference/data-types/array)                          | `List`                      |
| `Struct`                            | [Tuple](/reference/data-types/tuple)                          | `Struct`                    |
| `Null`                              | [Nullable(Nothing)](/reference/data-types/special-data-types/nothing) | `Null`             |

Other types are not supported. In particular, [Map](/reference/data-types/map),
[Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint), [IPv6](/reference/data-types/ipv6)
and [Interval](/reference/data-types/special-data-types/interval) columns cannot be written to Vortex files.
[String](/reference/data-types/string) columns are written as `Binary` because ClickHouse strings are
arbitrary byte sequences, while Vortex requires `Utf8` values to be valid UTF-8. Vortex has no
fixed-size binary type, so [FixedString](/reference/data-types/fixedstring) is also written as `Binary`,
and [LowCardinality](/reference/data-types/lowcardinality) columns are written as their underlying type
(Vortex chooses dictionary and other encodings adaptively by itself).
[DateTime](/reference/data-types/datetime) columns are written as `vortex.timestamp` with second precision,
so they are read back as [DateTime64](/reference/data-types/datetime64) with scale 0.
[IPv4](/reference/data-types/ipv4) columns are written as `U32` because Vortex has no type for IP addresses,
so schema inference reads them back as [UInt32](/reference/data-types/int-uint). Specify the type explicitly
to read such a column back as `IPv4`: `SELECT * FROM file('data.vortex', Vortex, 'ip IPv4')`.

The data types of ClickHouse table columns do not have to match the corresponding Vortex data fields.
When inserting data, ClickHouse interprets data types according to the table above and then
[casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to the data type set for the
ClickHouse table column.

## Example usage {#example-usage}

You can select data from a Vortex file:

```sql
SELECT * FROM file('data.vortex', Vortex);
```

And write data to a Vortex file:

```sql
SELECT * FROM numbers(3) INTO OUTFILE 'numbers.vortex' FORMAT Vortex;
```

## Format settings {#format-settings}

The format has no dedicated settings. As in other columnar formats, only the columns used by the
query are read from the file, and columns missing in the file are filled with default values.
)DOCS_MD"});
}

void registerVortexSchemaReader(FormatFactory & factory);
void registerVortexSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Vortex",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            return std::make_shared<VortexSchemaReader>(buf, settings);
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatVortex(FormatFactory &);
void registerInputFormatVortex(FormatFactory &) {}

void registerVortexSchemaReader(FormatFactory &);
void registerVortexSchemaReader(FormatFactory &) {}
}

#endif
