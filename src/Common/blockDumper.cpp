#include <Common/blockDumper.h>

#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

void dumpBlockToJSON(const Block & block, WriteBuffer & buffer)
{
    if (block.empty())
    {
        buffer.write("{}", 2);
        return;
    }

    auto materialized_block = materializeBlock(block);
    auto output_format = FormatFactory::instance().getDefaultJSONEachRowOutputFormat(buffer, materialized_block);
    output_format->write(materialized_block);
    output_format->finalize();
}

void dumpChunkToJSON(const Chunk & chunk, const Block & header, WriteBuffer & buffer)
{
    if (chunk.empty())
    {
        buffer.write("{}", 2);
        return;
    }

    Block block = header.cloneWithColumns(chunk.getColumns());
    dumpBlockToJSON(block, buffer);
}

}
