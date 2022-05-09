#include <Processors/Formats/Impl/JSONColumnsBaseBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <Formats/JSONUtils.h>


namespace DB
{

JSONColumnsBaseBlockOutputFormat::JSONColumnsBaseBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , format_settings(format_settings_)
    , serializations(header_.getSerializations())
    , ostr(&out)
{
}

void JSONColumnsBaseBlockOutputFormat::consume(Chunk chunk)
{
    if (!mono_chunk)
    {
        mono_chunk = std::move(chunk);
        return;
    }

    mono_chunk.append(chunk);
}

void JSONColumnsBaseBlockOutputFormat::writeSuffix()
{

    writeChunk(mono_chunk);
    mono_chunk.clear();
}

void JSONColumnsBaseBlockOutputFormat::writeChunk(Chunk & chunk)
{
    writeChunkStart();
    const auto & columns = chunk.getColumns();
    for (size_t i = 0; i != columns.size(); ++i)
    {
        writeColumnStart(i);
        writeColumn(*columns[i], *serializations[i]);
        writeColumnEnd(i == columns.size() - 1);
    }
    writeChunkEnd();
}

void JSONColumnsBaseBlockOutputFormat::writeColumnEnd(bool is_last)
{
    writeJSONCompactArrayEnd(*ostr);
    if (!is_last)
        writeJSONFieldDelimiter(*ostr);
}

void JSONColumnsBaseBlockOutputFormat::writeColumn(const IColumn & column, const ISerialization & serialization)
{
    for (size_t i = 0; i != column.size(); ++i)
    {
        if (i != 0)
            writeJSONFieldCompactDelimiter(*ostr);
        serialization.serializeTextJSON(column, i, *ostr, format_settings);
    }
}

}
