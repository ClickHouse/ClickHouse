#pragma once
#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{

class SourceFromSingleChunk : public SourceWithProgress
{
public:
    explicit SourceFromSingleChunk(Block header, Chunk chunk_);
    explicit SourceFromSingleChunk(Block data);
    String getName() const override { return "SourceFromSingleChunk"; }

protected:
    Chunk generate() override { return std::move(chunk); }

private:
    Chunk chunk;
};

}
