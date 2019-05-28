#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <IO/SharedReadBuffer.h>
#include <DataStreams/UnionBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT;
}


const FormatFactory::Creators & FormatFactory::getCreators(const String & name) const
{
    auto it = dict.find(name);
    if (dict.end() != it)
        return it->second;
    throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}


BlockInputStreamPtr FormatFactory::getInput(const String & name, ReadBuffer & buf, const Block & sample, const Context & context, UInt64 max_block_size, UInt64 rows_portion_size) const
{
    const auto & input_getter = getCreators(name).input_creator;
    if (!input_getter)
        throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);

    const Settings & settings = context.getSettingsRef();

    FormatSettings format_settings;
    format_settings.csv.delimiter = settings.format_csv_delimiter;
    format_settings.csv.allow_single_quotes = settings.format_csv_allow_single_quotes;
    format_settings.csv.allow_double_quotes = settings.format_csv_allow_double_quotes;
    format_settings.values.interpret_expressions = settings.input_format_values_interpret_expressions;
    format_settings.with_names_use_header = settings.input_format_with_names_use_header;
    format_settings.skip_unknown_fields = settings.input_format_skip_unknown_fields;
    format_settings.import_nested_json = settings.input_format_import_nested_json;
    format_settings.date_time_input_format = settings.date_time_input_format;
    format_settings.input_allow_errors_num = settings.input_format_allow_errors_num;
    format_settings.input_allow_errors_ratio = settings.input_format_allow_errors_ratio;

    const auto & file_segmentation_engine = getCreators(name).file_segmentation_engine;
    if (settings.input_format_parallel_parsing
            && !settings.input_format_defaults_for_omitted_fields
            && file_segmentation_engine)
    {
        size_t max_threads_to_use = settings.max_threads_for_parallel_reading;
        if (!max_threads_to_use)
            max_threads_to_use = settings.max_threads;

        BlockInputStreams streams;
        std::vector<std::unique_ptr<ReadBuffer>> buffers;
        streams.reserve(max_threads_to_use);
        buffers.reserve(max_threads_to_use);
        auto buf_mutex = std::make_shared<std::mutex>();
        for (size_t i = 0; i < max_threads_to_use; ++i)
        {
            buffers.emplace_back(std::make_unique<SharedReadBuffer>(buf, buf_mutex, file_segmentation_engine, settings.min_chunk_size_for_parallel_reading));
            streams.emplace_back(input_getter(*buffers.back(), sample, context, max_block_size, rows_portion_size, format_settings));
        }
        auto union_stream = std::make_shared<UnionBlockInputStream>(std::move(streams), nullptr, max_threads_to_use);
        union_stream->buffers = std::move(buffers);
        return union_stream;
    }

    return input_getter(buf, sample, context, max_block_size, rows_portion_size, format_settings);
}


BlockOutputStreamPtr FormatFactory::getOutput(const String & name, WriteBuffer & buf, const Block & sample, const Context & context) const
{
    const auto & output_getter = getCreators(name).output_creator;
    if (!output_getter)
        throw Exception("Format " + name + " is not suitable for output", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT);

    const Settings & settings = context.getSettingsRef();

    FormatSettings format_settings;
    format_settings.json.quote_64bit_integers = settings.output_format_json_quote_64bit_integers;
    format_settings.json.quote_denormals = settings.output_format_json_quote_denormals;
    format_settings.json.escape_forward_slashes = settings.output_format_json_escape_forward_slashes;
    format_settings.csv.delimiter = settings.format_csv_delimiter;
    format_settings.csv.allow_single_quotes = settings.format_csv_allow_single_quotes;
    format_settings.csv.allow_double_quotes = settings.format_csv_allow_double_quotes;
    format_settings.pretty.max_rows = settings.output_format_pretty_max_rows;
    format_settings.pretty.max_column_pad_width = settings.output_format_pretty_max_column_pad_width;
    format_settings.pretty.color = settings.output_format_pretty_color;
    format_settings.write_statistics = settings.output_format_write_statistics;
    format_settings.parquet.row_group_size = settings.output_format_parquet_row_group_size;

    /** Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    return std::make_shared<MaterializingBlockOutputStream>(
        output_getter(buf, sample, context, format_settings), sample);
}


void FormatFactory::registerInputFormat(const String & name, InputCreator input_creator)
{
    auto & target = dict[name].input_creator;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = input_creator;
}

void FormatFactory::registerOutputFormat(const String & name, OutputCreator output_creator)
{
    auto & target = dict[name].output_creator;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = output_creator;
}

void FormatFactory::registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine)
{
    auto & target = dict[name].file_segmentation_engine;
    if (target)
        throw Exception("FormatFactory: File segmentation engine " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = file_segmentation_engine;
}

/// Formats for both input/output.

void registerInputFormatNative(FormatFactory & factory);
void registerOutputFormatNative(FormatFactory & factory);
void registerInputFormatRowBinary(FormatFactory & factory);
void registerOutputFormatRowBinary(FormatFactory & factory);
void registerInputFormatTabSeparated(FormatFactory & factory);
void registerOutputFormatTabSeparated(FormatFactory & factory);
void registerInputFormatValues(FormatFactory & factory);
void registerOutputFormatValues(FormatFactory & factory);
void registerInputFormatCSV(FormatFactory & factory);
void registerOutputFormatCSV(FormatFactory & factory);
void registerInputFormatTSKV(FormatFactory & factory);
void registerOutputFormatTSKV(FormatFactory & factory);
void registerInputFormatJSONEachRow(FormatFactory & factory);
void registerOutputFormatJSONEachRow(FormatFactory & factory);
void registerInputFormatParquet(FormatFactory & factory);
void registerOutputFormatParquet(FormatFactory & factory);
void registerInputFormatProtobuf(FormatFactory & factory);
void registerOutputFormatProtobuf(FormatFactory & factory);

/// File Segmentation Engines for parallel reading

void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory);
void registerFileSegmentationEngineTabSeparated(FormatFactory & factory);
void registerFileSegmentationEngineValues(FormatFactory & factory);
void registerFileSegmentationEngineCSV(FormatFactory & factory);
void registerFileSegmentationEngineTSKV(FormatFactory & factory);

/// Output only (presentational) formats.

void registerOutputFormatPretty(FormatFactory & factory);
void registerOutputFormatPrettyCompact(FormatFactory & factory);
void registerOutputFormatPrettySpace(FormatFactory & factory);
void registerOutputFormatVertical(FormatFactory & factory);
void registerOutputFormatJSON(FormatFactory & factory);
void registerOutputFormatJSONCompact(FormatFactory & factory);
void registerOutputFormatXML(FormatFactory & factory);
void registerOutputFormatODBCDriver(FormatFactory & factory);
void registerOutputFormatODBCDriver2(FormatFactory & factory);
void registerOutputFormatNull(FormatFactory & factory);

/// Input only formats.

void registerInputFormatCapnProto(FormatFactory & factory);


FormatFactory::FormatFactory()
{
    registerInputFormatNative(*this);
    registerOutputFormatNative(*this);
    registerInputFormatRowBinary(*this);
    registerOutputFormatRowBinary(*this);
    registerInputFormatTabSeparated(*this);
    registerOutputFormatTabSeparated(*this);
    registerInputFormatValues(*this);
    registerOutputFormatValues(*this);
    registerInputFormatCSV(*this);
    registerOutputFormatCSV(*this);
    registerInputFormatTSKV(*this);
    registerOutputFormatTSKV(*this);
    registerInputFormatJSONEachRow(*this);
    registerOutputFormatJSONEachRow(*this);
    registerInputFormatProtobuf(*this);
    registerOutputFormatProtobuf(*this);
    registerInputFormatCapnProto(*this);
    registerInputFormatParquet(*this);
    registerOutputFormatParquet(*this);

    registerFileSegmentationEngineJSONEachRow(*this);
    registerFileSegmentationEngineTabSeparated(*this);
    registerFileSegmentationEngineValues(*this);
    registerFileSegmentationEngineCSV(*this);
    registerFileSegmentationEngineTSKV(*this);

    registerOutputFormatPretty(*this);
    registerOutputFormatPrettyCompact(*this);
    registerOutputFormatPrettySpace(*this);
    registerOutputFormatVertical(*this);
    registerOutputFormatJSON(*this);
    registerOutputFormatJSONCompact(*this);
    registerOutputFormatXML(*this);
    registerOutputFormatODBCDriver(*this);
    registerOutputFormatODBCDriver2(*this);
    registerOutputFormatNull(*this);
}

}
