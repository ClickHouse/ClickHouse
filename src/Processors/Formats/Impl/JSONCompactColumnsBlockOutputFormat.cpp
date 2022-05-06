#include <Processors/Formats/Impl/JSONCompactColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONCompactColumnsBlockOutputFormat::JSONCompactColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_)
    : JSONColumnsBaseBlockOutputFormat(out_, header_, format_settings_, mono_block_),  column_names(header_.getNames())
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
    for (const auto & [name, mono_block] : {std::make_pair("JSONCompactColumns", false), std::make_pair("JSONCompactColumnsMonoBlock", true)})
    {
        factory.registerOutputFormat(name, [mono_block = mono_block](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams &,
            const FormatSettings & format_settings)
        {
            return std::make_shared<JSONCompactColumnsBlockOutputFormat>(buf, sample, format_settings, mono_block);
        });
    }

    factory.markOutputFormatSupportsParallelFormatting("JSONCompactColumns");
}

}
