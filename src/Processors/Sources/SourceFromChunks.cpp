#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_)
    : SourceFromChunks(header, std::make_shared<Chunks>(std::move(chunks_)), true)
{}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_)
    : SourceFromChunks(header, chunks_, false)
{}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, bool move_from_chunks_)
    : ISource(std::move(header))
    , chunks(chunks_)
    , it(chunks->begin())
    , move_from_chunks(move_from_chunks_)
{
}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

Chunk SourceFromChunks::generate()
{
    if (it != chunks->end())
        if (move_from_chunks)
        {
            Chunk && chunk = std::move(*it);
            it++;
            return chunk;
        }
        else
        {
            Chunk chunk = it->clone();
            it++;
            return chunk;
        }
    else
        return {};
}

}

