#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SourceFromChunks::SourceFromChunks(SharedHeader header, Chunks chunks_)
    : ISource(std::move(header))
    , chunks(std::move(chunks_))
    , it(chunks.begin())
{}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

std::unique_ptr<SourceFromChunks> SourceFromChunks::clone() const
{
    /// `generate` moves each chunk out of `chunks` as it is produced, so a source that already
    /// started generating no longer holds the data a clone would have to replay.
    if (it != chunks.begin())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot clone {} after it started generating chunks", getName());

    Chunks cloned_chunks;
    cloned_chunks.reserve(chunks.size());
    for (const auto & chunk : chunks)
        cloned_chunks.push_back(chunk.clone());

    return std::make_unique<SourceFromChunks>(getPort().getSharedHeader(), std::move(cloned_chunks));
}

Chunk SourceFromChunks::generate()
{
    if (it != chunks.end())
    {
        Chunk && chunk = std::move(*it);
        ++it;
        return chunk;
    }
    return {};
}

}
