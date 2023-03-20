#pragma once
#include <Processors/IProcessor.h>
#include <Poco/Logger.h>

namespace DB
{
class InnerShuffleScatterTransform : public IProcessor
{
public:
    InnerShuffleScatterTransform(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_);
    String getName() const override { return "InnerShuffleScatterTransform"; }
    Status prepare() override;
    void work() override;

private:
    size_t num_streams;
    Block header;
    std::vector<size_t> hash_columns;
    bool has_output = false;
    std::list<Chunk> output_chunks;
    std::vector<std::list<Chunk>> pending_output_chunks;
    bool has_input = false;
    Chunk input_chunk;
};

class InnerShuffleGatherTransform : public IProcessor
{
public:
    InnerShuffleGatherTransform(const Block & header_, size_t inputs_num_);
    String getName() const override { return "InnerShuffleGatherTransform"; }
    Status prepare() override;
    void work() override;
private:
    bool has_input = false;
    bool has_output = false;
    std::list<Chunk> input_chunks;
    size_t pending_rows = 0;
    Chunk output_chunk;
    std::list<InputPort *> running_inputs;
    Chunk generateOneChunk();
};
}
