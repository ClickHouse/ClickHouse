#include <Common/blockDumper.h>

#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

String dumpBlockToJSON(const Block & block)
{
    if (block.empty())
        return "{}";

    auto materialized_block = materializeBlock(block);
    WriteBufferFromOwnString out;
    auto output_format = FormatFactory::instance().getDefaultJSONEachRowOutputFormat(out, materialized_block);
    output_format->write(materialized_block);
    output_format->finalize();

    return out.str();
}
String dumpChunkToJSON(const Chunk & chunk, const Block & header)
{
    if (chunk.empty())
        return "{}";

    Block block = header.cloneWithColumns(chunk.getColumns());
    return dumpBlockToJSON(block);
}

}
