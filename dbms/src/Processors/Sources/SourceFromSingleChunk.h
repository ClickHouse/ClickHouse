#pragma once
#include <Processors/ISource.h>


namespace DB
{

class SourceFromSingleChunk : public ISource
{
public:
    explicit SourceFromSingleChunk(Block header, Chunk chunk_) : ISource(std::move(header)), chunk(std::move(chunk_)) {}
    String getName() const override { return "SourceFromSingleChunk"; }

protected:
    Chunk generate() override { return std::move(chunk); }

private:
    Chunk chunk;
};

}
