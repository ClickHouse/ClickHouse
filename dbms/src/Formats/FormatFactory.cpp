#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowInputFormat.h>


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

const FormatFactory::ProcessorCreators & FormatFactory::getProcessorCreators(const String & name) const
{
    auto it = processors_dict.find(name);
    if (processors_dict.end() != it)
        return it->second;
    throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

static FormatSettings getInputFormatSetting(const Settings & settings)
{
    FormatSettings format_settings;
    format_settings.csv.delimiter = settings.format_csv_delimiter;
    format_settings.csv.allow_single_quotes = settings.format_csv_allow_single_quotes;
    format_settings.csv.allow_double_quotes = settings.format_csv_allow_double_quotes;
    format_settings.values.interpret_expressions = settings.input_format_values_interpret_expressions;
    format_settings.skip_unknown_fields = settings.input_format_skip_unknown_fields;
    format_settings.import_nested_json = settings.input_format_import_nested_json;
    format_settings.date_time_input_format = settings.date_time_input_format;
    format_settings.input_allow_errors_num = settings.input_format_allow_errors_num;
    format_settings.input_allow_errors_ratio = settings.input_format_allow_errors_ratio;

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
    format_settings.write_statistics = settings.output_format_write_statistics;

    return format_settings;
}


BlockInputStreamPtr FormatFactory::getInput(const String & name, ReadBuffer & buf, const Block & sample, const Context & context, UInt64 max_block_size) const
{
    const auto & input_getter = getCreators(name).first;
    if (!input_getter)
        throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);

    const Settings & settings = context.getSettingsRef();
    FormatSettings format_settings = getInputFormatSetting(settings);

    return input_getter(buf, sample, context, max_block_size, format_settings);
}


BlockOutputStreamPtr FormatFactory::getOutput(const String & name, WriteBuffer & buf, const Block & sample, const Context & context) const
{
    const auto & output_getter = getCreators(name).second;
    if (!output_getter)
        throw Exception("Format " + name + " is not suitable for output", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT);

    const Settings & settings = context.getSettingsRef();
    FormatSettings format_settings = getOutputFormatSetting(settings);

    /** Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    return std::make_shared<MaterializingBlockOutputStream>(
        output_getter(buf, sample, context, format_settings), sample);
}


InputFormatPtr FormatFactory::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, const Context & context, UInt64 max_block_size) const
{
    const auto & input_getter = getProcessorCreators(name).first;
    if (!input_getter)
        throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);

    const Settings & settings = context.getSettingsRef();
    FormatSettings format_settings = getInputFormatSetting(settings);

    RowInputFormatParams params;
    params.max_block_size = max_block_size;
    params.allow_errors_num = format_settings.input_allow_errors_num;
    params.allow_errors_ratio = format_settings.input_allow_errors_ratio;

    return input_getter(buf, sample, context, params, format_settings);
}


OutputFormatPtr FormatFactory::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample, const Context & context) const
{
    const auto & output_getter = getProcessorCreators(name).second;
    if (!output_getter)
        throw Exception("Format " + name + " is not suitable for output", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT);

    const Settings & settings = context.getSettingsRef();
    FormatSettings format_settings = getOutputFormatSetting(settings);

    /** TODO: Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    return output_getter(buf, sample, context, format_settings);
}


void FormatFactory::registerInputFormat(const String & name, InputCreator input_creator)
{
    auto & target = dict[name].first;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
}

void FormatFactory::registerOutputFormat(const String & name, OutputCreator output_creator)
{
    auto & target = dict[name].second;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(output_creator);
}

void FormatFactory::registerInputFormatProcessor(const String & name, InputProcessorCreator input_creator)
{
    auto & target = processors_dict[name].first;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
}

void FormatFactory::registerOutputFormatProcessor(const String & name, OutputProcessorCreator output_creator)
{
    auto & target = processors_dict[name].second;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(output_creator);
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
void registerOutputFormatProtobuf(FormatFactory & factory);

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
void registerOutputFormatProcessorProtobuf(FormatFactory & factory);

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

void registerOutputFormatProcessorPretty(FormatFactory & factory);
void registerOutputFormatProcessorPrettyCompact(FormatFactory & factory);
void registerOutputFormatProcessorPrettySpace(FormatFactory & factory);
void registerOutputFormatProcessorVertical(FormatFactory & factory);
void registerOutputFormatProcessorJSON(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompact(FormatFactory & factory);
void registerOutputFormatProcessorXML(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver2(FormatFactory & factory);
void registerOutputFormatProcessorNull(FormatFactory & factory);

/// Input only formats.

void registerInputFormatCapnProto(FormatFactory & factory);
void registerInputFormatProcessorCapnProto(FormatFactory & factory);


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
    registerOutputFormatProtobuf(*this);
    registerInputFormatCapnProto(*this);

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
    registerOutputFormatProcessorProtobuf(*this);
    registerInputFormatProcessorCapnProto(*this);

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
}

}
