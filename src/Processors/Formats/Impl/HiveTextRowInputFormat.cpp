#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>

#if USE_HIVE

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


static FormatSettings updateFormatSettings(const FormatSettings & settings)
{
    FormatSettings updated = settings;
    updated.csv.delimiter = updated.hive_text.fields_delimiter;
    return updated;
}

HiveTextRowInputFormat::HiveTextRowInputFormat(
    const Block & header_, ReadBuffer & in_, const Params & params_, const FormatSettings & format_settings_)
    : HiveTextRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, updateFormatSettings(format_settings_))
{
}

HiveTextRowInputFormat::HiveTextRowInputFormat(
    const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_, const FormatSettings & format_settings_)
    : CSVRowInputFormat(
        header_, *buf_, params_, true, false, format_settings_, std::make_unique<HiveTextFormatReader>(std::move(buf_), format_settings_))
{
}

HiveTextFormatReader::HiveTextFormatReader(std::unique_ptr<PeekableReadBuffer> buf_, const FormatSettings & format_settings_)
    : CSVFormatReader(*buf_, format_settings_), buf(std::move(buf_)), input_field_names(format_settings_.hive_text.input_field_names)
{
}

std::vector<String> HiveTextFormatReader::readNames()
{
    PeekableReadBufferCheckpoint checkpoint{*buf, true};
    auto values = readHeaderRow();
    input_field_names.resize(values.size());
    return input_field_names;
}

std::vector<String> HiveTextFormatReader::readTypes()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "HiveTextRowInputFormat::readTypes is not implemented");
}

void registerInputFormatHiveText(FormatFactory & factory)
{
    factory.registerInputFormat(
        "HiveText", [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        {
            return std::make_shared<HiveTextRowInputFormat>(sample, buf, params, settings);
        });
}
}
#endif
