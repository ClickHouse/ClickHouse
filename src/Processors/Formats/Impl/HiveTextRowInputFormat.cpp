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
{
}

void HiveTextRowInputFormat::readPrefix()
{
    std::vector<bool> read_columns(data_types.size(), false);
    /// For Hive Text file, read the first row to get exact number of columns.
    auto values = readNames();
    input_field_names = format_settings.csv.input_field_names;
    input_field_names.resize(values.size());
    for (const auto & column_name : input_field_names)
        addInputColumn(column_name, read_columns);

    for (size_t i = 0; i != read_columns.size(); ++i)
    {
        if (!read_columns[i])
            column_mapping->not_presented_columns.push_back(i);
    }
}

std::vector<String> HiveTextRowInputFormat::readNames()
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    auto values = readHeaderRow();
    buf.rollbackToCheckpoint();
    return values;
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
            return std::make_shared<HiveTextRowInputFormat>(sample, buf, params, settings);
        });
}
}
#endif
