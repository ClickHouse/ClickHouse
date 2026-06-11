#pragma once

#include <Processors/ISource.h>


namespace DB
{

class SourceFromSingleChunk : public ISource
{
/// If the source consists of multiple chunks you can instead use SourceFromChunks.
public:
    SourceFromSingleChunk(SharedHeader header, Chunk chunk_);
    explicit SourceFromSingleChunk(SharedHeader data);
    String getName() const override;

protected:
    Chunk generate() override;

private:
    Chunk chunk;
};

}
