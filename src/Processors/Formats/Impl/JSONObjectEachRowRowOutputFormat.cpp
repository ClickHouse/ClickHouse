#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>
#include <Formats/JSONUtils.h>
#include <IO/WriteHelpers.h>

namespace DB
{

JSONObjectEachRowRowOutputFormat::JSONObjectEachRowRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_)
    : JSONEachRowRowOutputFormat(out_, header_, params_, settings_)
{
}

void JSONObjectEachRowRowOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
}

void JSONObjectEachRowRowOutputFormat::writeRowStartDelimiter()
{
    ++row_num;
    String title = "row_" + std::to_string(row_num);
    JSONUtils::writeCompactObjectStart(*ostr, 1, title.c_str());
}

void JSONObjectEachRowRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeCompactObjectEnd(*ostr);
    field_number = 0;
}

void JSONObjectEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    JSONUtils::writeFieldDelimiter(*ostr, 1);
}

void JSONObjectEachRowRowOutputFormat::writeSuffix()
{
    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONObjectEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONObjectEachRow", [](
                       WriteBuffer & buf,
                       const Block & sample,
                       const RowOutputFormatParams & params,
                       const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONObjectEachRowRowOutputFormat>(buf, sample, params, settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("JSONObjectEachRow");
}

}
