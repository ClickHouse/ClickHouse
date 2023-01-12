#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_)
    : SourceFromChunks(header, std::make_shared<Chunks>(std::move(chunks_)))
{
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<const Chunks> chunks_)
    : ISource(std::move(header))
    , chunks(chunks_)
{
}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

Chunk SourceFromChunks::generate()
{
    if (i_chunk < chunks->size())
    {
        Chunk chunk = ((*chunks)[i_chunk]).clone();
        i_chunk++;
        return chunk;
    }
    else
        return {};
}

}

