#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISource.h>


namespace DB
{

/// The big brother of SourceFromSingleChunk. Supports multiple chunks and totals/extremes.
class SourceFromChunks : public ISource
{
public:
    SourceFromChunks(Block header, Chunks && chunks_, std::optional<Chunk> && chunk_totals_, std::optional<Chunk> && chunk_extremes_);

    String getName() const override;

    Status prepare() override;
    void work() override;

    OutputPort * getTotalsPort() const { return output_totals; }
    OutputPort * getExtremesPort() const { return output_extremes; }

    Chunk generate() override;

private:
    Chunks chunks;
    Chunks::iterator it;

    std::optional<Chunk> chunk_totals = std::nullopt;
    std::optional<Chunk> chunk_extremes = std::nullopt;

    OutputPort * output_totals = nullptr;
    OutputPort * output_extremes = nullptr;

    bool finished_chunks = false;
};

}
