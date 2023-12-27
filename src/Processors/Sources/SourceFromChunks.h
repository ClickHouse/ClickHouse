#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISource.h>


namespace DB
{

class SourceFromChunks : public ISource
{
public:
    SourceFromChunks(Block header, Chunks chunks_);
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_);

    String getName() const override;

protected:
    Chunk generate() override;

private:
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, bool move_from_chunks_);

    const std::shared_ptr<Chunks> chunks;
    Chunks::iterator it;
    /// Optimization: if the chunks are exclusively owned by SourceFromChunks, then generate() can move from them
    const bool move_from_chunks;
};

}
