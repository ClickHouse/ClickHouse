#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONColumnsBlockOutputFormat::JSONColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, size_t indent_)
    : JSONColumnsBlockOutputFormatBase(out_, header_, format_settings_), fields(header_.getNamesAndTypes()), indent(indent_)
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
    JSONUtils::writeObjectStart(*ostr, indent);
}

void JSONColumnsBlockOutputFormat::writeColumnStart(size_t column_index)
{
    JSONUtils::writeCompactArrayStart(*ostr, indent + 1, fields[column_index].name.data());
}

void JSONColumnsBlockOutputFormat::writeChunkEnd()
{
    JSONUtils::writeObjectEnd(*ostr, indent);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONColumns(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONColumns", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONColumnsBlockOutputFormat>(buf, sample, format_settings);
    });
}

}
