#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISource.h>


namespace DB
{

class SourceFromChunks : public ISource
{
public:
    SourceFromChunks(Block header, Chunks chunks_);
    SourceFromChunks(Block header, std::shared_ptr<const Chunks> chunks_);
    String getName() const override;

protected:
    Chunk generate() override;

private:
    std::shared_ptr<const Chunks> chunks;
    size_t i_chunk = 0;
};

}
