#pragma once
#include <deque>
#include <Processors/IProcessor.h>

namespace DB
{

/// Transform which has single input and num_outputs outputs.
/// Read chunk from input and copy it to all outputs.
class CopyTransform : public IProcessor
{
public:
    CopyTransform(const Block & header, size_t num_outputs);

    String getName() const override { return "Copy"; }
    Status prepare() override;

    InputPort & getInputPort() { return inputs.front(); }

private:
    Chunk chunk;
    bool has_data = false;
    std::vector<char> was_output_processed;

    Status prepareGenerate();
    Status prepareConsume();
};


class CopyAccumulatingTransform : public IProcessor
{
public:
    CopyAccumulatingTransform(const Block & header, size_t num_outputs);

    String getName() const override { return "CopyAccumulating"; }
    Status prepare() override;

    InputPort & getInputPort() { return inputs.front(); }

private:
    std::deque<Chunk> chunks;
    size_t init_idx = 0;
    bool has_data = false;
    std::vector<size_t> outputs_chunk_index;

    Status prepareGenerate();
    Status prepareConsume();
};

}
