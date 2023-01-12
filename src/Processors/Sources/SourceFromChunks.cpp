#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_)
    : ISource(std::move(header))
    , chunks(std::move(chunks_))
{
}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

Chunk SourceFromChunks::generate()
{
    if (i_chunk < chunks.size())
        return std::move(chunks[i_chunk++]);
    else
        return {};
}

}

