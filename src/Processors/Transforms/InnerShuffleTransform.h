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
    std::vector<Chunk> chunks;
};

// Split one chunk into multiple chunks according to the hash value of the specified columns.
// And pass a list of chunks to InnerShuffleDispatchTransform.
class InnerShuffleScatterTransformV2 : public IProcessor
{
public:
    InnerShuffleScatterTransformV2(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_);
    ~InnerShuffleScatterTransformV2() override = default;
    String getName() const override { return "InnerShuffleScatterTransformV2"; }
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

// Collect all splitted chunks from multiple InnerShuffleScatterTransformV2 and dispatch them into
// corresponding partitions.
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

class InnerShuffleGatherTransformV2 : public IProcessor
{
public:
    InnerShuffleGatherTransformV2(const Block & header_, size_t input_num_);
    ~InnerShuffleGatherTransformV2() override = default;
    String getName() const override { return "InnerShuffleGatherTransformV2"; }
    Status prepare() override;
    void work() override;
private:
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
    std::vector<InputPort *> input_port_ptrs;
    size_t input_port_iter = 0;
};

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
    size_t pending_chunks = 0;
    Chunk output_chunk;
    std::list<InputPort *> running_inputs;
    Chunk generateOneChunk();
};
}
