#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONColumnsBlockOutputFormat::JSONColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool validate_utf8, size_t indent_)
    : JSONColumnsBlockOutputFormatBase(out_, header_, format_settings_, validate_utf8), indent(indent_)
{
    names = JSONUtils::makeNamesValidJSONStrings(header_.getNames(), format_settings, validate_utf8);
}

void JSONColumnsBlockOutputFormat::writeChunkStart()
{
    JSONUtils::writeObjectStart(*ostr, indent);
}

void JSONColumnsBlockOutputFormat::writeColumnStart(size_t column_index)
{
    JSONUtils::writeCompactArrayStart(*ostr, indent + 1, names[column_index].data());
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
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONColumnsBlockOutputFormat>(buf, sample, format_settings, format_settings.json.validate_utf8);
    });
}

}
