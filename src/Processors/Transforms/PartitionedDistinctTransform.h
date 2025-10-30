#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/SetVariants.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <sys/types.h>

namespace DB
{

class PartitionDistinctTransformBase : public IProcessor
{
public:
    enum class PortStatus : uint8_t
    {
        NotActive,
        NeedData,
        Blocked,
        Finished,
    };
    template <typename T>
    struct PortWithStatus
    {
        T * port = nullptr;
        PortStatus status = PortStatus::NotActive;
        PortWithStatus(T * port_, PortStatus status_)
            : port(port_)
            , status(status_)
        {
        }
        PortWithStatus() = default;
    };
    using InputPortWithStatus = PortWithStatus<InputPort>;
    using OutputPortWithStatus = PortWithStatus<OutputPort>;
    using ChunkQueue = std::queue<Chunk>;
    using Status = IProcessor::Status;

    PartitionDistinctTransformBase(SharedHeader header_, size_t input_streams_, size_t output_streams_);
    virtual ~PartitionDistinctTransformBase() override = default;
    virtual String getName() const override { return "PartitionDistinctTransformBase"; }
    Status prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports) override;
    virtual void work() override { }

protected:
    SharedHeader header;
    size_t input_streams;
    size_t output_streams;

    std::vector<ChunkQueue> input_chunks;
    std::vector<ChunkQueue> output_chunks;

    std::vector<InputPortWithStatus> input_ports;
    size_t finished_input_ports = 0;
    size_t pending_input_chunks_num = 0;

    std::vector<OutputPortWithStatus> output_ports;
    std::queue<size_t> waiting_output_ports;
    size_t finished_output_ports = 0;
    size_t pending_output_chunks_num = 0;
};


// One input, multiple output streams transform to do partitioned distinct.
class PartitionDistinctMapTransform : public PartitionDistinctTransformBase
{
public:
    PartitionDistinctMapTransform(SharedHeader header_, const Names & key_columns_names_, size_t output_streams_);
    ~PartitionDistinctMapTransform() override = default;
    String getName() const override { return "PartitionDistinctMapTransform"; }

    void work() override;

private:
    ColumnNumbers key_columns_pos;

    std::vector<std::shared_ptr<detail::Selector>> scatterBlock(const Block & block);
    Blocks scatterBlockByCopying(const ColumnNumbers & key_columns_pos, const Block & block);
    bool use_copy_scatter = true;
};

class PartitionDistinctReduceTransform : public PartitionDistinctTransformBase
{
public:
    using Status = IProcessor::Status;
    PartitionDistinctReduceTransform(SharedHeader header_, const Names & key_columns_names_, size_t input_streams_);
    ~PartitionDistinctReduceTransform() override = default;
    String getName() const override { return "PartitionDistinctReduceTransform"; }
    void work() override;

private:
    ColumnNumbers key_columns_pos;
    Sizes key_column_sizes;
    SetVariants set_variants;
    size_t next_input_stream = 0;
};
}
