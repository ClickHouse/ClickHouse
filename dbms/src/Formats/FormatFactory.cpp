#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Formats/OutputStreamToOutputFormat.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>


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


static FormatSettings getInputFormatSetting(const Settings & settings)
{
    FormatSettings format_settings;
    format_settings.csv.delimiter = settings.format_csv_delimiter;
    format_settings.csv.allow_single_quotes = settings.format_csv_allow_single_quotes;
    format_settings.csv.allow_double_quotes = settings.format_csv_allow_double_quotes;
    format_settings.csv.unquoted_null_literal_as_null = settings.input_format_csv_unquoted_null_literal_as_null;
    format_settings.csv.empty_as_default = settings.input_format_defaults_for_omitted_fields;
    format_settings.csv.null_as_default = settings.input_format_null_as_default;
    format_settings.values.interpret_expressions = settings.input_format_values_interpret_expressions;
    format_settings.with_names_use_header = settings.input_format_with_names_use_header;
    format_settings.skip_unknown_fields = settings.input_format_skip_unknown_fields;
    format_settings.import_nested_json = settings.input_format_import_nested_json;
    format_settings.date_time_input_format = settings.date_time_input_format;
    format_settings.input_allow_errors_num = settings.input_format_allow_errors_num;
    format_settings.input_allow_errors_ratio = settings.input_format_allow_errors_ratio;
    format_settings.template_settings.format = settings.format_schema;
    format_settings.template_settings.row_format = settings.format_schema_rows;
    format_settings.template_settings.row_between_delimiter = settings.format_schema_rows_between_delimiter;

    return format_settings;
}

static FormatSettings getOutputFormatSetting(const Settings & settings)
{
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
    format_settings.template_settings.format = settings.format_schema;
    format_settings.template_settings.row_format = settings.format_schema_rows;
    format_settings.template_settings.row_between_delimiter = settings.format_schema_rows_between_delimiter;
    format_settings.write_statistics = settings.output_format_write_statistics;
    format_settings.parquet.row_group_size = settings.output_format_parquet_row_group_size;

    return format_settings;
}


BlockInputStreamPtr FormatFactory::getInput(
    const String & name,
    ReadBuffer & buf,
    const Block & sample,
    const Context & context,
    UInt64 max_block_size,
    UInt64 rows_portion_size,
    ReadCallback callback) const
{
    if (name == "Native")
        return std::make_shared<NativeBlockInputStream>(buf, sample, 0);

    if (!getCreators(name).input_processor_creator)
    {
        const auto & input_getter = getCreators(name).inout_creator;
        if (!input_getter)
            throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);

        const Settings & settings = context.getSettingsRef();
        FormatSettings format_settings = getInputFormatSetting(settings);

        return input_getter(
                buf, sample, context, max_block_size, rows_portion_size, callback ? callback : ReadCallback(), format_settings);
    }

    auto format = getInputFormat(name, buf, sample, context, max_block_size, rows_portion_size, std::move(callback));
    return std::make_shared<InputStreamFromInputFormat>(std::move(format));
}


BlockOutputStreamPtr FormatFactory::getOutput(
    const String & name, WriteBuffer & buf, const Block & sample, const Context & context, WriteCallback callback) const
{
    if (name == "PrettyCompactMonoBlock")
    {
        /// TODO: rewrite
        auto format = getOutputFormat("PrettyCompact", buf, sample, context);
        auto res = std::make_shared<SquashingBlockOutputStream>(
                std::make_shared<OutputStreamToOutputFormat>(format),
                sample, context.getSettingsRef().output_format_pretty_max_rows, 0);

        res->disableFlush();

        return std::make_shared<MaterializingBlockOutputStream>(res, sample);
    }

    if (!getCreators(name).output_processor_creator)
    {
        const auto & output_getter = getCreators(name).output_creator;
        if (!output_getter)
            throw Exception("Format " + name + " is not suitable for output", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT);

        const Settings & settings = context.getSettingsRef();
        FormatSettings format_settings = getOutputFormatSetting(settings);

        /**  Materialization is needed, because formats can use the functions `IDataType`,
          *  which only work with full columns.
          */
        return std::make_shared<MaterializingBlockOutputStream>(
                output_getter(buf, sample, context, callback, format_settings), sample);
    }

    auto format = getOutputFormat(name, buf, sample, context, callback);
    return std::make_shared<MaterializingBlockOutputStream>(std::make_shared<OutputStreamToOutputFormat>(format), sample);
}


InputFormatPtr FormatFactory::getInputFormat(
    const String & name,
    ReadBuffer & buf,
    const Block & sample,
    const Context & context,
    UInt64 max_block_size,
    UInt64 rows_portion_size,
    ReadCallback callback) const
{
    const auto & input_getter = getCreators(name).input_processor_creator;
    if (!input_getter)
        throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);

    const Settings & settings = context.getSettingsRef();
    FormatSettings format_settings = getInputFormatSetting(settings);

    RowInputFormatParams params;
    params.max_block_size = max_block_size;
    params.allow_errors_num = format_settings.input_allow_errors_num;
    params.allow_errors_ratio = format_settings.input_allow_errors_ratio;
    params.rows_portion_size = rows_portion_size;
    params.callback = std::move(callback);
    params.max_execution_time = settings.max_execution_time;
    params.timeout_overflow_mode = settings.timeout_overflow_mode;

    return input_getter(buf, sample, context, params, format_settings);
}


OutputFormatPtr FormatFactory::getOutputFormat(
    const String & name, WriteBuffer & buf, const Block & sample, const Context & context, WriteCallback callback) const
{
    const auto & output_getter = getCreators(name).output_processor_creator;
    if (!output_getter)
        throw Exception("Format " + name + " is not suitable for output", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT);

    const Settings & settings = context.getSettingsRef();
    FormatSettings format_settings = getOutputFormatSetting(settings);

    /** TODO: Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    return output_getter(buf, sample, context, callback, format_settings);
}


void FormatFactory::registerInputFormat(const String & name, InputCreator input_creator)
{
    auto & target = dict[name].inout_creator;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
}

void FormatFactory::registerOutputFormat(const String & name, OutputCreator output_creator)
{
    auto & target = dict[name].output_creator;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(output_creator);
}

void FormatFactory::registerInputFormatProcessor(const String & name, InputProcessorCreator input_creator)
{
    auto & target = dict[name].input_processor_creator;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
}

void FormatFactory::registerOutputFormatProcessor(const String & name, OutputProcessorCreator output_creator)
{
    auto & target = dict[name].output_processor_creator;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(output_creator);
}


/// Formats for both input/output.

void registerInputFormatNative(FormatFactory & factory);
void registerOutputFormatNative(FormatFactory & factory);

void registerInputFormatProcessorNative(FormatFactory & factory);
void registerOutputFormatProcessorNative(FormatFactory & factory);
void registerInputFormatProcessorRowBinary(FormatFactory & factory);
void registerOutputFormatProcessorRowBinary(FormatFactory & factory);
void registerInputFormatProcessorTabSeparated(FormatFactory & factory);
void registerOutputFormatProcessorTabSeparated(FormatFactory & factory);
void registerInputFormatProcessorValues(FormatFactory & factory);
void registerOutputFormatProcessorValues(FormatFactory & factory);
void registerInputFormatProcessorCSV(FormatFactory & factory);
void registerOutputFormatProcessorCSV(FormatFactory & factory);
void registerInputFormatProcessorTSKV(FormatFactory & factory);
void registerOutputFormatProcessorTSKV(FormatFactory & factory);
void registerInputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerInputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorORC(FormatFactory & factory);
void registerOutputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorProtobuf(FormatFactory & factory);
void registerOutputFormatProcessorProtobuf(FormatFactory & factory);
void registerInputFormatProcessorTemplate(FormatFactory & factory);
void registerOutputFormatProcessorTemplate(FormatFactory &factory);

/// Output only (presentational) formats.

void registerOutputFormatNull(FormatFactory & factory);

void registerOutputFormatProcessorPretty(FormatFactory & factory);
void registerOutputFormatProcessorPrettyCompact(FormatFactory & factory);
void registerOutputFormatProcessorPrettySpace(FormatFactory & factory);
void registerOutputFormatProcessorVertical(FormatFactory & factory);
void registerOutputFormatProcessorJSON(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompact(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatProcessorXML(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver2(FormatFactory & factory);
void registerOutputFormatProcessorNull(FormatFactory & factory);
void registerOutputFormatProcessorMySQLWrite(FormatFactory & factory);

/// Input only formats.
void registerInputFormatProcessorCapnProto(FormatFactory & factory);

FormatFactory::FormatFactory()
{
    registerInputFormatNative(*this);
    registerOutputFormatNative(*this);

    registerOutputFormatProcessorJSONEachRowWithProgress(*this);

    registerInputFormatProcessorNative(*this);
    registerOutputFormatProcessorNative(*this);
    registerInputFormatProcessorRowBinary(*this);
    registerOutputFormatProcessorRowBinary(*this);
    registerInputFormatProcessorTabSeparated(*this);
    registerOutputFormatProcessorTabSeparated(*this);
    registerInputFormatProcessorValues(*this);
    registerOutputFormatProcessorValues(*this);
    registerInputFormatProcessorCSV(*this);
    registerOutputFormatProcessorCSV(*this);
    registerInputFormatProcessorTSKV(*this);
    registerOutputFormatProcessorTSKV(*this);
    registerInputFormatProcessorJSONEachRow(*this);
    registerOutputFormatProcessorJSONEachRow(*this);
    registerInputFormatProcessorProtobuf(*this);
    registerOutputFormatProcessorProtobuf(*this);
    registerInputFormatProcessorCapnProto(*this);
    registerInputFormatProcessorORC(*this);
    registerInputFormatProcessorParquet(*this);
    registerOutputFormatProcessorParquet(*this);
    registerInputFormatProcessorTemplate(*this);
    registerOutputFormatProcessorTemplate(*this);


    registerOutputFormatNull(*this);

    registerOutputFormatProcessorPretty(*this);
    registerOutputFormatProcessorPrettyCompact(*this);
    registerOutputFormatProcessorPrettySpace(*this);
    registerOutputFormatProcessorVertical(*this);
    registerOutputFormatProcessorJSON(*this);
    registerOutputFormatProcessorJSONCompact(*this);
    registerOutputFormatProcessorXML(*this);
    registerOutputFormatProcessorODBCDriver(*this);
    registerOutputFormatProcessorODBCDriver2(*this);
    registerOutputFormatProcessorNull(*this);
    registerOutputFormatProcessorMySQLWrite(*this);
}

}
