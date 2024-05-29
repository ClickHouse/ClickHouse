#pragma once
#include <Processors/IProcessor.h>
#include <queue>

namespace DB
{

class BufferChunksTransform : public IProcessor
{
public:
    BufferChunksTransform(const Block & header_, size_t num_ports_, size_t max_bytes_to_buffer_, size_t limit_);

    String getName() const override { return "BufferChunks"; }
    Status prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs) override;

private:
    Chunk pullChunk(size_t input_idx);

    size_t max_bytes_to_buffer;
    size_t limit;

    struct InputPortWithStatus
    {
        InputPort * port;
        bool is_finished;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        bool is_finished;
    };

    std::vector<std::queue<Chunk>> chunks;
    std::vector<size_t> num_processed_rows;

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    std::queue<UInt64> available_outputs;

    bool is_reading_started = false;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    size_t num_buffered_bytes = 0;
};

}
