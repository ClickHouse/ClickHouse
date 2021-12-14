#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>

#if USE_HIVE

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

HiveTextRowInputFormat::HiveTextRowInputFormat(
    const Block & header_, ReadBuffer & in_, const Params & params_, const FormatSettings & format_settings_)
    : CSVRowInputFormat(header_, buf, params_, true, false, format_settings_)
    , buf(in_)
    , input_field_names(format_settings_.hive_text.input_field_names)
{
}

std::vector<String> HiveTextRowInputFormat::readNames()
{
    PeekableReadBufferCheckpoint checkpoint{buf, true};
    auto values = readHeaderRow();
    input_field_names.resize(values.size());
    return input_field_names;
}

std::vector<String> HiveTextRowInputFormat::readTypes()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "HiveTextRowInputFormat::readTypes is not implemented");
}

void registerInputFormatHiveText(FormatFactory & factory)
{
    factory.registerInputFormat(
        "HiveText",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        {
            FormatSettings settings_copy = settings;
            settings_copy.csv.delimiter = settings_copy.hive_text.fields_delimiter;
            return std::make_shared<HiveTextRowInputFormat>(sample, buf, params, settings_copy);
        });
}
}
#endif
