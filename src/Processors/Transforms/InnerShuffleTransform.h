#pragma once
#include <Processors/IProcessor.h>
#include <Poco/Logger.h>

namespace DB
{

class InnerShuffleScatterChunkInfo : public ChunkInfo
{
public:
    size_t finished_streams = 0;
    size_t count = 0;
    // scatter result from InnerShuffleScatterTransform. chunks.size() == num_streams
    std::vector<Chunk> chunks;
};

// Split one chunk into multiple chunks according to the hash value of the specified columns.
// And pass a list of chunks to InnerShuffleDispatchTransform.
class InnerShuffleScatterTransform : public IProcessor
{
public:
    InnerShuffleScatterTransform(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_);
    ~InnerShuffleScatterTransform() override = default;
    String getName() const override { return "InnerShuffleScatterTransform"; }
    Status prepare() override;
    void work() override;
private:
    size_t num_streams;
    Block header;
    std::vector<size_t> hash_columns;
    bool has_output = false;
    std::vector<Chunk> output_chunks;
    bool has_input = false;
    Chunk input_chunk;
};

// Collect all hash split chunks from multiple InnerShuffleScatterTransform and dispatch them
// into corresponding partitions.
class InnerShuffleDispatchTransform : public IProcessor
{
public:
    InnerShuffleDispatchTransform(size_t input_nums_, size_t output_nums_, const Block & header_);
    ~InnerShuffleDispatchTransform() override = default;
    String getName() const override { return "InnerShuffleDispatchTransform"; }
    Status prepare() override;
    void work() override;
private:
    size_t input_nums;
    size_t output_nums;
    Block header;
    bool has_input = false;
    std::vector<Chunk> input_chunks;
    std::vector<std::list<Chunk>> output_chunks;
};

// Collect result from InnerShuffleDispatchTransforms, make sure that each stream will be handled by
// one thread.
class InnerShuffleGatherTransform : public IProcessor
{
public:
    InnerShuffleGatherTransform(const Block & header_, size_t input_num_);
    ~InnerShuffleGatherTransform() override = default;
    String getName() const override { return "InnerShuffleGatherTransform"; }
    Status prepare() override;
    void work() override;
private:
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
    std::vector<InputPort *> input_port_ptrs;
    size_t input_port_iter = 0;
};
}
