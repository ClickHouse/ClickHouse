#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

namespace DB
{

class Block;

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
    Port::Data data;
    std::vector<char> was_output_processed;

    Status prepareGenerate();
    Status prepareConsume();
};

}
