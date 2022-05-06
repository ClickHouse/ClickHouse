#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONColumnsBlockOutputFormat::JSONColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_, size_t indent_)
    : JSONColumnsBaseBlockOutputFormat(out_, header_, format_settings_, mono_block_), fields(header_.getNamesAndTypes()), indent(indent_)
{
    for (auto & field : fields)
    {
        WriteBufferFromOwnString buf;
        writeJSONString(field.name, buf, format_settings);
        field.name = buf.str().substr(1, buf.str().size() - 2);
    }
}

void JSONColumnsBlockOutputFormat::writeChunkStart()
{
    writeJSONObjectStart(*ostr, indent);
}

void JSONColumnsBlockOutputFormat::writeColumnStart(size_t column_index)
{
    writeJSONCompactArrayStart(*ostr, indent + 1, fields[column_index].name.data());
}

void JSONColumnsBlockOutputFormat::writeChunkEnd()
{
    writeJSONObjectEnd(*ostr, indent);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONColumns(FormatFactory & factory)
{
    for (const auto & [name, mono_block] : {std::make_pair("JSONColumns", false), std::make_pair("JSONColumnsMonoBlock", true)})
    {
        factory.registerOutputFormat(name, [mono_block = mono_block](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams &,
            const FormatSettings & format_settings)
        {
            return std::make_shared<JSONColumnsBlockOutputFormat>(buf, sample, format_settings, mono_block);
        });
    }

    factory.markOutputFormatSupportsParallelFormatting("JSONColumns");
}

}
