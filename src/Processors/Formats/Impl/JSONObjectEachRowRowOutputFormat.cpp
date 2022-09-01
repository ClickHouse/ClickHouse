#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>
#include <Formats/JSONUtils.h>

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
    JSONUtils::writeObjectStart(*ostr, 1, "row");
}

void JSONObjectEachRowRowOutputFormat::writeSuffix()
{
    JSONUtils::writeObjectEnd(*ostr);
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
        return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, params, settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("JSONObjectEachRow");
}

}
