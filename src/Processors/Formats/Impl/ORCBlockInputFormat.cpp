#include "ORCBlockInputFormat.h"
#include "Common/Exception.h"

#if USE_ORC
#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/SchemaInferenceUtils.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/WriteHelpers.h>
#    include <IO/copyData.h>
#    include <boost/algorithm/string/case_conv.hpp>
#    include "ArrowBufferedStreams.h"
#    include "ArrowColumnToCHColumn.h"
#    include "ArrowFieldIndexUtil.h"
#    include "NativeORCBlockInputFormat.h"
#    include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
    , skip_stripes(format_settings.orc.skip_stripes)
{
}

Chunk ORCBlockInputFormat::read()
{
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    for (; stripe_current < stripe_total && skip_stripes.contains(stripe_current); ++stripe_current)
        ;

    if (stripe_current >= stripe_total)
        return {};

    if (need_only_count)
        return getChunkForCount(file_reader->GetRawORCReader()->getStripe(stripe_current++)->getNumberOfRows());

    auto batch_result = file_reader->ReadStripe(stripe_current, include_indices);
    if (!batch_result.ok())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader: {}", batch_result.status().ToString());

    auto batch = batch_result.ValueOrDie();
    if (!batch)
        return {};

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_result.status().ToString());

    /// We should extract the number of rows directly from the stripe, because in case when
    /// record batch contains 0 columns (for example if we requested only columns that
    /// are not presented in data) the number of rows in record batch will be 0.
    size_t num_rows = file_reader->GetRawORCReader()->getStripe(stripe_current)->getNumberOfRows();

    const auto & table = table_result.ValueOrDie();
    if (!table || !num_rows)
        return {};

    approx_bytes_read_for_chunk = file_reader->GetRawORCReader()->getStripe(stripe_current)->getDataLength();
    ++stripe_current;

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    std::shared_ptr<const arrow::KeyValueMetadata> metadata;
    if (auto status = file_reader->ReadMetadata(); status.ok())
        metadata = status.ValueOrDie();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected error while reading parquet metadata {}", status.status().message());

    return arrow_column_to_ch_column->arrowTableToCHChunk(table, num_rows, metadata, block_missing_values_ptr);
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    block_missing_values.clear();
}

const BlockMissingValues * ORCBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}


static void getFileReaderAndSchema(
    ReadBuffer & in,
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    if (is_stopped)
        return;

    auto result = arrow::adapters::orc::ORCFileReader::Open(arrow_file, ArrowMemoryPool::instance());
    if (!result.ok())
        throw Exception::createDeprecated(result.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    file_reader = std::move(result).ValueOrDie();

    auto read_schema_result = file_reader->ReadSchema();
    if (!read_schema_result.ok())
        throw Exception::createDeprecated(read_schema_result.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    schema = std::move(read_schema_result).ValueOrDie();
}

void ORCBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    stripe_total = static_cast<int>(file_reader->NumberOfStripes());
    stripe_current = 0;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "ORC",
        format_settings,
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser,
        format_settings.orc.case_insensitive_column_matching);

    const bool ignore_case = format_settings.orc.case_insensitive_column_matching;
    std::unordered_set<String> nested_table_names = Nested::getAllTableNames(getPort().getHeader(), ignore_case);
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name, ignore_case) || nested_table_names.contains(ignore_case ? boost::to_lower_copy(name) : name))
            include_indices.push_back(i);
    }
}

ORCSchemaReader::ORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

void ORCSchemaReader::initializeIfNeeded()
{
    if (file_reader)
        return;

    if (auto status = file_reader->ReadMetadata(); status.ok())
        metadata = status.ValueUnsafe();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading incorrect metadata of ORC {}", status.status().message());

    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);
}

NamesAndTypesList ORCSchemaReader::readSchema()
{
    initializeIfNeeded();

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema,
        metadata,
        "ORC",
        format_settings,
        format_settings.orc.skip_columns_with_unsupported_types_in_schema_inference,
        format_settings.schema_inference_make_columns_nullable != 0,
        false,
        format_settings.parquet.allow_geoparquet_parser);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> ORCSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return file_reader->NumberOfRows();
}

void registerInputFormatORC(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ORC",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool is_remote_fs,
           FormatParserGroupPtr parser_group)
        {
            InputFormatPtr res;
            if (settings.orc.use_fast_decoder)
            {
                const bool has_file_size = isBufferWithFileSize(buf);
                auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(&buf);
                const bool use_prefetch = is_remote_fs && read_settings.remote_fs_prefetch && has_file_size && seekable_in
                    && seekable_in->checkIfActuallySeekable() && seekable_in->supportsReadAt() && settings.seekable_read;
                const size_t min_bytes_for_seek = use_prefetch ? read_settings.remote_read_min_bytes_for_seek : 0;
                res = std::make_shared<NativeORCBlockInputFormat>(buf, sample, settings, use_prefetch, min_bytes_for_seek, parser_group);
            }
            else
                res = std::make_shared<ORCBlockInputFormat>(buf, sample, settings);

            return res;
        });
    factory.markFormatSupportsSubsetOfColumns("ORC");
}

void registerORCSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ORC",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            SchemaReaderPtr res;
            if (settings.orc.use_fast_decoder)
                res = std::make_shared<NativeORCSchemaReader>(buf, settings);
            else
                res = std::make_shared<ORCSchemaReader>(buf, settings);

            return res;
        }
        );

    factory.registerAdditionalInfoForSchemaCacheGetter("ORC", [](const FormatSettings & settings)
    {
        return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable);
    });
}

}
#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatORC(FormatFactory &)
    {
    }

    void registerORCSchemaReader(FormatFactory &)
    {
    }
}

#endif
