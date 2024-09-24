#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Chunk.h>
#include <memory>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class NotJoinedBlocks;
class IBlocksStream;
using IBlocksStreamPtr = std::shared_ptr<IBlocksStream>;

/// Join rows to chunk form left table.
/// This transform usually has two input ports and one output.
/// First input is for data from left table.
/// Second input has empty header and is connected with FillingRightJoinSide.
/// We can process left table only when Join is filled. Second input is used to signal that FillingRightJoinSide is finished.
class JoiningTransform : public IProcessor
{
public:

    /// Count streams and check which is last.
    /// The last one should process non-joined rows.
    class FinishCounter
    {
    public:
        explicit FinishCounter(size_t total_) : total(total_) {}

        bool isLast()
        {
            return finished.fetch_add(1) + 1 >= total;
        }

    private:
        const size_t total;
        std::atomic<size_t> finished{0};
    };

    using FinishCounterPtr = std::shared_ptr<FinishCounter>;

    JoiningTransform(
        const Block & input_header,
        const Block & output_header,
        JoinPtr join_,
        size_t max_block_size_,
        bool on_totals_ = false,
        bool default_totals_ = false,
        FinishCounterPtr finish_counter_ = nullptr);

    ~JoiningTransform() override;

    String getName() const override { return "JoiningTransform"; }

    static Block transformHeader(Block header, const JoinPtr & join);

    OutputPort & getFinishedSignal();

    Status prepare() override;
    void work() override;

protected:
    void transform(Chunk & chunk);

private:
    Chunk input_chunk;
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
    bool stop_reading = false;
    bool process_non_joined = true;

    JoinPtr join;
    bool on_totals;
    /// This flag means that we have manually added totals to our pipeline.
    /// It may happen in case if joined subquery has totals, but out string doesn't.
    /// We need to join default values with subquery totals if we have them, or return empty chunk is haven't.
    bool default_totals;
    bool initialized = false;

    ExtraBlockPtr not_processed;

    FinishCounterPtr finish_counter;
    IBlocksStreamPtr non_joined_blocks;
    size_t max_block_size;

    Block readExecute(Chunk & chunk);
};

/// Fills Join with block from right table.
/// Has single input and single output port.
/// Output port has empty header. It is closed when all data is inserted in join.
class FillingRightJoinSideTransform : public IProcessor
{
public:
    FillingRightJoinSideTransform(Block input_header, JoinPtr join_);
    String getName() const override { return "FillingRightJoinSide"; }

    InputPort * addTotalsPort();

    Status prepare() override;
    void work() override;

private:
    JoinPtr join;
    Chunk chunk;
    bool stop_reading = false;
    bool for_totals = false;
    bool set_totals = false;
};


class DelayedBlocksTask : public ChunkInfoCloneable<DelayedBlocksTask>
{
public:

    DelayedBlocksTask() = default;
    DelayedBlocksTask(const DelayedBlocksTask & other) = default;
    explicit DelayedBlocksTask(IBlocksStreamPtr delayed_blocks_, JoiningTransform::FinishCounterPtr left_delayed_stream_finish_counter_)
        : delayed_blocks(std::move(delayed_blocks_))
        , left_delayed_stream_finish_counter(left_delayed_stream_finish_counter_)
    {
    }

    IBlocksStreamPtr delayed_blocks = nullptr;
    JoiningTransform::FinishCounterPtr left_delayed_stream_finish_counter = nullptr;

};

using DelayedBlocksTaskPtr = std::shared_ptr<const DelayedBlocksTask>;


/// Reads delayed joined blocks from Join
class DelayedJoinedBlocksTransform : public IProcessor
{
public:
    explicit DelayedJoinedBlocksTransform(size_t num_streams, JoinPtr join_);

    String getName() const override { return "DelayedJoinedBlocksTransform"; }

    Status prepare() override;
    void work() override;

private:
    JoinPtr join;

    IBlocksStreamPtr delayed_blocks = nullptr;
    bool finished = false;
};

class DelayedJoinedBlocksWorkerTransform : public IProcessor
{
public:
    using NonJoinedStreamBuilder = std::function<IBlocksStreamPtr()>;
    explicit DelayedJoinedBlocksWorkerTransform(
        Block output_header_,
        NonJoinedStreamBuilder non_joined_stream_builder_);

    String getName() const override { return "DelayedJoinedBlocksWorkerTransform"; }

    Status prepare() override;
    void work() override;

private:
    DelayedBlocksTaskPtr task;
    Chunk output_chunk;
    /// For building a block stream to access the non-joined rows.
    NonJoinedStreamBuilder non_joined_stream_builder;
    IBlocksStreamPtr non_joined_delayed_stream = nullptr;

    void resetTask();
    Block nextNonJoinedBlock();
};

}
