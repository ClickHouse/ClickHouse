#pragma once
#include <Processors/IProcessor.h>
#include <Common/PODArray.h>


namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class NotJoinedBlocks;

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
        Block input_header,
        JoinPtr join_,
        size_t max_block_size_,
        bool on_totals_ = false,
        bool default_totals_ = false,
        FinishCounterPtr finish_counter_ = nullptr);

    String getName() const override { return "JoiningTransform"; }

    static Block transformHeader(Block header, const JoinPtr & join);

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
    std::shared_ptr<NotJoinedBlocks> non_joined_blocks;
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


class ParallelJoinTransform : public IProcessor
{
public:
    ParallelJoinTransform(JoinPtr join_, size_t num_inputs_)
        // : IProcessor({}, {Block()})
        : num_inputs(num_inputs_),
          inp(0),
          current_input(inputs.begin()),
          status(num_inputs),
          // first_input(true),
          join(join_)
    {
        // for (size_t i = 0; i < num_inputs; ++i)
        //     inputs.emplace_back(Block(), this);
        // current_input = inputs.begin();
    }

    String getName() const override { return "ParallelJoinTransform"; }
    void addHeader(Block input_header);
    Block getHeader();
    void mk_ports();

    InputPort * addTotalsPort();

protected:
    Status prepare() override;
    void work() override;

private:
    // Chunk current_input_chunk;
    // Chunk current_output_chunk;
    // InputPorts::iterator current_input;
    size_t num_inputs;
    size_t inp;
    InputPorts::iterator current_input;

    struct SourceStatus
    {
        size_t left_pos;
        size_t current_pos;
        bool left_rest;
        bool right_rest;
        Block block;
        PaddedPODArray<Int16> right_filt;
        //  1  -  filter the row out
        //  negative -    -1 * (left_position + 1)
    };

    std::vector<SourceStatus> status;

    IColumn::Filter left_filt;


    JoinPtr join;
    Chunk chunk;
    Blocks input_headers;


};

}
