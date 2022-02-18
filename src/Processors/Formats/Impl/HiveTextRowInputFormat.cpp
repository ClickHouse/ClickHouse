#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

#if USE_HIVE

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


FormatSettings HiveTextRowInputFormat::updateFormatSettings(const FormatSettings & settings)
{
    FormatSettings updated = settings;
    updated.csv.delimiter = updated.hive_text.fields_delimiter;
    updated.csv.allow_single_quotes = false;
    updated.csv.allow_double_quotes = false;
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

bool HiveTextFormatReader::readField(
    IColumn & column,
    [[maybe_unused]] const DataTypePtr & type,
    [[maybe_unused]] const SerializationPtr & serialization,
    [[maybe_unused]] bool is_last_file_column,
    [[maybe_unused]] const String & column_name)
{
    String s;
    readStringUntilChars(s, *in, {format_settings.hive_text.fields_delimiter});
    ReadBufferFromString fbuf(s);
    serialization->deserializeTextHiveText(column, fbuf, format_settings);
    return true;
}

void HiveTextFormatReader::skipFieldDelimiter()
{
    assertChar(format_settings.hive_text.fields_delimiter, *in);
}

void HiveTextFormatReader::skipRowEndDelimiter()
{
    if (in->eof())
        return;
    skipEndOfLine(*in);
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
