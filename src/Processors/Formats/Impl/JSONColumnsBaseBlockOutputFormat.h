#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;

/// Base class for Columnar JSON output formats.
/// It outputs data block by block. If mono_block_ argument is true,
/// it will buffer up to output_format_json_columns_max_rows_to_buffer rows
/// and outputs them as a single block in writeSuffix() method.
class JSONColumnsBaseBlockOutputFormat : public IOutputFormat
{
public:
    JSONColumnsBaseBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_);

    String getName() const override { return "JSONColumnsBaseBlockOutputFormat"; }

protected:
    void consume(Chunk chunk) override;
    void writeSuffix() override;

    void writeChunk(Chunk & chunk);
    void writeColumn(const IColumn & column, const ISerialization & serialization);

    virtual void writeChunkStart() = 0;
    virtual void writeChunkEnd() = 0;
    virtual void writeColumnStart(size_t /*column_index*/) = 0;
    void writeColumnEnd(bool is_last);

    const FormatSettings format_settings;
    bool mono_block;
    Serializations serializations;

    WriteBuffer * ostr;

    /// For mono_block == true only
    Chunk mono_chunk;
    size_t max_rows_in_mono_block;
    size_t total_rows_in_mono_block = 0;
};

}
