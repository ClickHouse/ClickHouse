#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;

/// Base class for Columnar JSON output formats.
/// It buffers all data and outputs it as a single block in writeSuffix() method.
class JSONColumnsBaseBlockOutputFormat : public IOutputFormat
{
public:
    JSONColumnsBaseBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

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
    Serializations serializations;

    WriteBuffer * ostr;

    Chunk mono_chunk;
};

}
