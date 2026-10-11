#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISource.h>


namespace DB
{

/// The big brother of SourceFromSingleChunk.
class SourceFromChunks final : public ISource
{
public:
    SourceFromChunks(SharedHeader header, Chunks chunks_);

    String getName() const override;

    /// Returns an independent source over the same chunks. Cloning is cheap: `Chunk::clone` shares
    /// the underlying (immutable) columns. Only valid before generation started, because `generate`
    /// moves the chunks out of the source; otherwise `NOT_IMPLEMENTED` is thrown so callers such as
    /// `FutureSetFromSubquery::buildOrderedSetInplace` take their non-clonable fallback.
    std::unique_ptr<SourceFromChunks> clone() const;

protected:
    Chunk generate() override;

private:
    Chunks chunks;
    Chunks::iterator it;
};

}
