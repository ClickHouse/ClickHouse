#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/IProcessor.h>
#include <Interpreters/Squashing.h>

namespace DB
{


class PlanSquashingTransform : public IProcessor
{
public:
    PlanSquashingTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports);

    String getName() const override { return "PlanSquashingTransform"; }

    InputPorts & getInputPorts() { return inputs; }
    OutputPorts & getOutputPorts() { return outputs; }

    Status prepare() override;
    Status prepareConsume();
    Status prepareSend();

    void transform(Chunk & chunk);

protected:

private:
    size_t CalculateBlockSize(const Block & block);
    Chunk chunk;
    PlanSquashing balance;
    bool has_data = false;
    std::vector<char> was_output_processed;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};
}

