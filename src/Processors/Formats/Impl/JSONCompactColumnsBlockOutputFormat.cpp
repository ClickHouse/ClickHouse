#include <Processors/Formats/Impl/JSONCompactColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONCompactColumnsBlockOutputFormat::JSONCompactColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : JSONColumnsBaseBlockOutputFormat(out_, header_, format_settings_), column_names(header_.getNames())
{
}

void JSONCompactColumnsBlockOutputFormat::writeChunkStart()
{
    writeJSONArrayStart(*ostr);
}

void JSONCompactColumnsBlockOutputFormat::writeColumnStart(size_t)
{
    writeJSONCompactArrayStart(*ostr, 1);
}

void JSONCompactColumnsBlockOutputFormat::writeChunkEnd()
{
    writeJSONArrayEnd(*ostr);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONCompactColumns(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONCompactColumns", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactColumnsBlockOutputFormat>(buf, sample, format_settings);
    });
}

}
