#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include "Processors/Chunk.h"
#include "Processors/IProcessor.h"
#include "Processors/Transforms/ExceptionKeepingTransform.h"
#include <Interpreters/SquashingTransform.h>

namespace DB
{

class BalancingTransform : public ExceptionKeepingTransform
{
public:
    explicit BalancingTransform(
        const Block & header, size_t max_memory_usage_);

    String getName() const override { return "BalancingTransform"; }

    void work() override;

    const Chunks & getChunks() const
    {
        return chunks;
    }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    void onFinish() override;

private:
    size_t CalculateBlockSize(const Block & block);
    Chunks chunks;
    Blocks blocks;
    size_t blocks_size;
    Chunk cur_chunk;
    Chunk finish_chunk;
    size_t max_memory_usage;
};

/// Doesn't care about propagating exceptions and thus doesn't throw LOGICAL_ERROR if the following transform closes its input port.


/// Doesn't care about propagating exceptions and thus doesn't throw LOGICAL_ERROR if the following transform closes its input port.
class LBalancingChunksTransform : public ISimpleTransform
{
public:
    explicit LBalancingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage, bool skip_empty_chunks_);

    String getName() const override { return "LBalancingChunksTransform"; }

    const Chunks & getChunks() const
    {
        return chunks;
    }

protected:
    void transform(Chunk &) override;

    IProcessor::Status prepare() override;

private:
    size_t CalculateBlockSize(const Block & block);
    [[maybe_unused]] ChunksToSquash chunks_to_merge;
    Chunks chunks;
    Blocks blocks;
    [[maybe_unused]] size_t blocks_size;
    Chunk cur_chunk;
    Chunk finish_chunk;
    [[maybe_unused]] size_t max_memory_usage;
    SquashingTransform squashing;
    BalanceTransform balance;
    [[maybe_unused]]size_t acc_size = 0;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};

class BalancingChunksTransform : public IProcessor
{
public:
    BalancingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage_, size_t num_ports);
    // explicit BalancingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t max_memory_usage, bool skip_empty_chunks_);

    String getName() const override { return "BalancingChunksTransform"; }

    const Chunks & getChunks() const
    {
        return chunks;
    }
    
    InputPorts & getInputPorts() { return inputs; }
    OutputPorts & getOutputPorts() { return outputs; }

    Status prepare() override;
    Status prepareConsume();
    Status prepareSend();

    // void work() override;
    void transform(Chunk & chunk);

protected:
    // void transform(Chunk &) ;

private:
    size_t CalculateBlockSize(const Block & block);
    [[maybe_unused]] ChunksToSquash chunks_to_merge;
    Chunks chunks;
    Chunk chunk;
    Blocks blocks;
    [[maybe_unused]] size_t blocks_size;
    Chunk cur_chunk;
    Chunk finish_chunk;
    [[maybe_unused]] size_t max_memory_usage;
    SquashingTransform squashing;
    BalanceTransform balance;
    [[maybe_unused]]size_t acc_size = 0;
    bool has_data = false;
    std::vector<char> was_output_processed;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};
}

