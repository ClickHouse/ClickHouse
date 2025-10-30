#include <Processors/Formats/Impl/JSONCompactColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <Processors/Port.h>

namespace DB
{

JSONCompactColumnsBlockOutputFormat::JSONCompactColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : JSONColumnsBlockOutputFormatBase(out_, header_, format_settings_, format_settings_.json.validate_utf8), column_names(header_.getNames())
{
}

void JSONCompactColumnsBlockOutputFormat::writeChunkStart()
{
    JSONUtils::writeArrayStart(*ostr);
}

void JSONCompactColumnsBlockOutputFormat::writeColumnStart(size_t)
{
    JSONUtils::writeCompactArrayStart(*ostr, 1);
}

void JSONCompactColumnsBlockOutputFormat::writeChunkEnd()
{
    JSONUtils::writeArrayEnd(*ostr);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONCompactColumns(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONCompactColumns", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactColumnsBlockOutputFormat>(buf, sample, format_settings);
    });
    factory.setContentType("JSONCompactColumns", "application/json; charset=UTF-8");
}

}
