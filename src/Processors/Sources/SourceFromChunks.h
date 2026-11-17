#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISource.h>


namespace DB
{

/// The big brother of SourceFromSingleChunk.
class SourceFromChunks : public ISource
{
public:
    SourceFromChunks(SharedHeader header, Chunks chunks_);

    String getName() const override;

protected:
    Chunk generate() override;

private:
    Chunks chunks;
    Chunks::iterator it;
};

}
