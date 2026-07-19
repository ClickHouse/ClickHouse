#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <Core/Block.h>
#include <Processors/Port.h>

namespace DB
{

JSONColumnsBlockOutputFormat::JSONColumnsBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, bool validate_utf8, size_t indent_)
    : JSONColumnsBlockOutputFormatBase(out_, header_, format_settings_, validate_utf8), indent(indent_), header(header_)
{
    names = JSONUtils::makeNamesValidJSONStrings(header_->getNames(), format_settings, validate_utf8);
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
    /// Write empty chunk
    if (!written_rows)
    {
        const auto & columns = header->getColumns();
        for (size_t i = 0; i != columns.size(); ++i)
        {
            writeColumnStart(i);
            writeColumn(*columns[i], *serializations[i]);
            writeColumnEnd(i == columns.size() - 1);
        }
    }

    JSONUtils::writeObjectEnd(*ostr, indent);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONColumns(FormatFactory & factory);
void registerOutputFormatJSONColumns(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONColumns", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONColumnsBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, format_settings.json.validate_utf8);
    });

    factory.setContentType("JSONColumns", "application/json; charset=UTF-8");

    /// The column names are emitted as JSON object keys via `makeNamesValidJSONStrings` with
    /// `output_format_json_validate_utf8`. When validation is off, a name that is not valid UTF-8
    /// (a quoted alias with arbitrary bytes) makes the keys, and hence the output, non-textual. It is
    /// knowable from the header, so the text framings reject or base64-encode the output accordingly.
    factory.registerOutputFormatMayProduceRawBytesChecker(
        "JSONColumns",
        [](const FormatSettings & settings, const Block & header)
        {
            return JSONUtils::namesMayProduceRawBytesInJSON(header.getNames(), settings, settings.json.validate_utf8);
        });
}

}
