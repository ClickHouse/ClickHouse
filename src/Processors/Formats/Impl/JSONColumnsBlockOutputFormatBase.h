#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;

/// Base class for Columnar JSON output formats.
/// It buffers all data and outputs it as a single block in writeSuffix() method.
class JSONColumnsBlockOutputFormatBase : public IOutputFormat
{
public:
    JSONColumnsBlockOutputFormatBase(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "JSONColumnsBlockOutputFormatBase"; }

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
    const Serializations serializations;

    WriteBuffer * ostr;

    Chunk mono_chunk;
};

}
