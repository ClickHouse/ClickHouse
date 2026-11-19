#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Splits every chunk by ranges of a key column into parts for the outputs: equal shares of its rows in the order of the key.
/// For a join that looks its keys up in a storage where a lookup reads the data around its keys (see `IJoin::getLookupBatching`):
/// the parts are looked up in parallel and read disjoint granules.
/// The order of rows is kept within a part, but not between the outputs.
class ScatterByKeyRangesTransform final : public IProcessor
{
public:
    ScatterByKeyRangesTransform(SharedHeader header, size_t num_outputs, size_t key_position_);

    String getName() const override { return "ScatterByKeyRanges"; }

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return inputs.front(); }

private:
    /// Hands the parts of the chunk over to the outputs that can take them. Returns true once none is left.
    bool pushParts();

    const size_t key_position;

    Chunk chunk;
    Chunks parts;
};

}
