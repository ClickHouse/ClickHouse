#pragma once

#include <Interpreters/SquashingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class SquashingChunksTransform : public ExceptionKeepingTransform
{
public:
    explicit SquashingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "SquashingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    void onFinish() override;

private:
    NewSquashingTransform squashing;
    Chunk cur_chunk;
    Chunk finish_chunk;
};

/// Doesn't care about propagating exceptions and thus doesn't throw LOGICAL_ERROR if the following transform closes its input port.
class SimpleSquashingChunksTransform : public ISimpleTransform
{
public:
    explicit SimpleSquashingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool skip_empty_chunks_ = true);

    String getName() const override { return "SimpleSquashingTransform"; }

protected:
    void transform(Chunk &) override;

    IProcessor::Status prepare() override;

private:
    NewSquashingTransform squashing;

    bool finished = false;
};


class ProcessorSquashingTransform : public IProcessor
{
public:
    explicit ProcessorSquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports);

    String getName() const override { return "ProcessorSquashingTransform"; }

protected:
    InputPorts & getInputPorts() { return inputs; }
    OutputPorts & getOutputPorts() { return outputs; }

    Status prepare() override;
    Status prepareConsume();
    Status prepareGenerate();

    // void work() override;
    void transform(Chunk & chunk);

private:
    NewSquashingTransform squashing;
    Chunk chunk;
    bool has_data = false;
    std::vector<char> was_output_processed;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};
}
