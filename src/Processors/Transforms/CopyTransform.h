#pragma once
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

}
