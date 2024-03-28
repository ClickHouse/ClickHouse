#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/IProcessor.h>
#include <Interpreters/SquashingTransform.h>

namespace DB
{


class BalancingChunksTransform : public IProcessor
{
public:
    BalancingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage_, size_t num_ports);

    String getName() const override { return "BalancingChunksTransform"; }

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
    size_t max_memory_usage;
    BalanceTransform balance;
    bool has_data = false;
    std::vector<char> was_output_processed;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};
}

