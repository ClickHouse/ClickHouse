#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_)
    : ISource(std::move(header))
    , chunks(std::move(chunks_))
    , it(chunks.begin())
{}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

Chunk SourceFromChunks::generate()
{
    if (it != chunks.end())
    {
        Chunk && chunk = std::move(*it);
        it++;
        return chunk;
    }
    return {};
}

}
