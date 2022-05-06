#include <Processors/Formats/Impl/JSONColumnsBaseBlockOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <Formats/JSONUtils.h>


namespace DB
{

JSONColumnsBaseBlockOutputFormat::JSONColumnsBaseBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_)
    : IOutputFormat(header_, out_)
    , format_settings(format_settings_)
    , mono_block(mono_block_)
    , serializations(header_.getSerializations())
    , ostr(&out)
    , max_rows_in_mono_block(format_settings_.json_columns.max_rows_to_buffer)
{
}

void JSONColumnsBaseBlockOutputFormat::consume(Chunk chunk)
{
    if (!mono_block)
    {
        writeChunk(chunk);
        return;
    }

    if (!mono_chunk)
    {
        mono_chunk = std::move(chunk);
        total_rows_in_mono_block = mono_chunk.getNumRows();
        return;
    }

    /// Copy up to (max_rows_in_mono_block - total_rows_in_mono_block) rows.
    size_t length = chunk.getNumRows();
    if (total_rows_in_mono_block + length > max_rows_in_mono_block)
        length = max_rows_in_mono_block - total_rows_in_mono_block;
    mono_chunk.append(chunk, length);
    total_rows_in_mono_block += length;
}

void JSONColumnsBaseBlockOutputFormat::writeSuffix()
{
    if (mono_chunk)
    {
        writeChunk(mono_chunk);
        mono_chunk.clear();
    }
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
