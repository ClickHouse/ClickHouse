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
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of Arrow data: {}", rows.status().ToString());
            return getChunkForCount(*rows);
        }

        batch_result = file_reader->ReadRecordBatch(record_batch_current);
    }

    if (!batch_result.ok())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while reading batch of Arrow data: {}", batch_result.status().ToString());

    auto table_result = arrow::Table::FromRecordBatches({*batch_result});
    if (!table_result.ok())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while reading batch of Arrow data: {}", table_result.status().ToString());

    ++record_batch_current;

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    res = arrow_column_to_ch_column->arrowTableToCHChunk(*table_result, (*table_result)->num_rows(), file_reader ? file_reader->metadata() : nullptr, block_missing_values_ptr);

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
        int32_t first_int;
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
    options.memory_pool = arrow::default_memory_pool();
    auto stream_reader_status = arrow::ipc::RecordBatchStreamReader::Open(std::make_unique<ArrowInputStreamFromReadBuffer>(in), options);
    if (!stream_reader_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Error while opening a table: {}", stream_reader_status.status().ToString());
    return *stream_reader_status;
}

static std::shared_ptr<arrow::ipc::RecordBatchFileReader> createFileReader(ReadBuffer & in, const FormatSettings & format_settings, std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Arrow", ARROW_MAGIC_BYTES);
    if (is_stopped)
        return nullptr;

    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.memory_pool = arrow::default_memory_pool();
    auto file_reader_status = arrow::ipc::RecordBatchFileReader::Open(arrow_file, options);
    if (!file_reader_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Error while opening a table: {}", file_reader_status.status().ToString());
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
        file_reader ? file_reader->metadata() : nullptr,
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
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of Arrow data: {}", rows.status().ToString());

    return *rows;
}

void registerInputFormatArrow(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Arrow",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & /* params */,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ArrowBlockInputFormat>(buf, std::make_shared<const Block>(sample), false, format_settings);
        });
    factory.markFormatSupportsSubsetOfColumns("Arrow");
    factory.registerInputFormat(
        "ArrowStream",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & /* params */,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ArrowBlockInputFormat>(buf, std::make_shared<const Block>(sample), true, format_settings);
        });
}

void registerArrowSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Arrow",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<ArrowSchemaReader>(buf, false, settings);
        });

    factory.registerAdditionalInfoForSchemaCacheGetter("Arrow", [](const FormatSettings & settings)
    {
        return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable);
    });
    factory.registerSchemaReader(
        "ArrowStream",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<ArrowSchemaReader>(buf, true, settings);
        });

    factory.registerAdditionalInfoForSchemaCacheGetter("ArrowStream", [](const FormatSettings & settings)
    {
       return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable);
    });
}

}
#else

namespace DB
{
class FormatFactory;
void registerInputFormatArrow(FormatFactory &)
{
}

void registerArrowSchemaReader(FormatFactory &) {}
}

#endif
