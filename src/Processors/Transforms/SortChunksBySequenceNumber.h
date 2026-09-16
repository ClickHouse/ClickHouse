#pragma once

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Port.h>

namespace DB
{

struct AddSequenceNumber : ISimpleTransform
{
public:
    explicit AddSequenceNumber(SharedHeader header_)
        : ISimpleTransform(header_, header_, false)
    {
    }

    String getName() const override { return "AddSequenceNumber"; }

    void transform(Chunk & chunk) override;

private:
    UInt64 chunk_sequence_number = 0;
};

class SortChunksBySequenceNumber : public IProcessor
{
public:
    SortChunksBySequenceNumber(const Block & header_, size_t num_inputs_);

    String getName() const override { return "SortChunksBySequenceNumber"; }

    IProcessor::Status prepare() final;

    bool tryPushChunk();

    void addChunk(Chunk chunk, size_t input);

private:
    const size_t num_inputs;
    std::vector<Int64> chunk_snums;
    std::vector<Chunk> chunks;
    std::vector<bool> is_input_finished;
};

}
