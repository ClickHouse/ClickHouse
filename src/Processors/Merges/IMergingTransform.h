#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/IProcessor.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>

namespace DB
{

/// Base class for IMergingTransform.
/// It is needed to extract all non-template methods in single translation unit.
class IMergingTransformBase : public IProcessor
{
public:
    IMergingTransformBase(
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_);

    IMergingTransformBase(
        const Blocks & input_headers,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_);

    OutputPort & getOutputPort() { return outputs.front(); }

    /// Methods to add additional input port. It is possible to do only before the first call of `prepare`.
    void addInput();
    /// Need to be called after all inputs are added. (only if have_all_inputs was not specified).
    void setHaveAllInputs();

    Status prepare() override;

protected:
    virtual void onNewInput(); /// Is called when new input is added. Only if have_all_inputs = false.
    virtual void onFinish() {} /// Is called when all data is processed.

    /// Processor state.
    struct State
    {
        Chunk output_chunk;
        IMergingAlgorithm::Input input_chunk;
        bool has_input = false;
        bool is_finished = false;
        bool need_data = false;
        bool no_data = false;
        size_t next_input_to_read = 0;

        IMergingAlgorithm::Inputs init_chunks;
    };

    State state;

private:
    struct InputState
    {
        explicit InputState(InputPort & port_) : port(port_) {}

        InputPort & port;
        bool is_initialized = false;
    };

    std::vector<InputState> input_states;
    std::atomic<bool> have_all_inputs;
    bool is_initialized = false;
    UInt64 limit_hint = 0;
    bool always_read_till_end = false;

    IProcessor::Status prepareInitializeInputs();
};

/// Implementation of MergingTransform using IMergingAlgorithm.
template <typename Algorithm>
class IMergingTransform : public IMergingTransformBase
{
public:
    template <typename ... Args>
    IMergingTransform(
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_,
        Args && ... args)
        : IMergingTransformBase(num_inputs, input_header, output_header, have_all_inputs_, limit_hint_, always_read_till_end_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    template <typename ... Args>
    IMergingTransform(
        const Blocks & input_headers,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_,
        bool empty_chunk_on_finish_,
        Args && ... args)
        : IMergingTransformBase(input_headers, output_header, have_all_inputs_, limit_hint_, always_read_till_end_)
        , empty_chunk_on_finish(empty_chunk_on_finish_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    void work() override
    {
        Stopwatch watch{CLOCK_MONOTONIC_COARSE};

        if (!state.init_chunks.empty())
            algorithm.initialize(std::move(state.init_chunks));

        if (state.has_input)
        {
            // std::cerr << "Consume chunk with " << state.input_chunk.getNumRows()
            //           << " for input " << state.next_input_to_read << std::endl;
            algorithm.consume(state.input_chunk, state.next_input_to_read);
            state.has_input = false;
        }
        else if (state.no_data && empty_chunk_on_finish)
        {
            IMergingAlgorithm::Input current_input;
            algorithm.consume(current_input, state.next_input_to_read);
            state.no_data = false;
        }

        IMergingAlgorithm::Status status = algorithm.merge();

        if ((status.chunk && status.chunk.hasRows()) || !status.chunk.getChunkInfos().empty())
        {
            // std::cerr << "Got chunk with " << status.chunk.getNumRows() << " rows" << std::endl;
            state.output_chunk = std::move(status.chunk);
        }

        if (status.required_source >= 0)
        {
            // std::cerr << "Required data for input " << status.required_source << std::endl;
            state.next_input_to_read = status.required_source;
            state.need_data = true;
        }

        if (status.is_finished)
        {
            // std::cerr << "Finished" << std::endl;
            state.is_finished = true;
        }

        merging_elapsed_ns += watch.elapsedNanoseconds();
    }

protected:
    /// Call `consume` with empty chunk when there is no more data.
    bool empty_chunk_on_finish = false;

    Algorithm algorithm;

    /// Profile info.
    UInt64 merging_elapsed_ns = 0;

    void logMergedStats(ProfileEvents::Event elapsed_ms_event, std::string_view transform_message, LoggerPtr log) const
    {
        auto stats = algorithm.getMergedStats();

        UInt64 elapsed_ms = merging_elapsed_ns / 1000000LL;
        ProfileEvents::increment(elapsed_ms_event, elapsed_ms);

        /// Don't print info for small parts (< 1M rows)
        if (stats.rows < 1000000)
            return;

        double seconds = static_cast<double>(merging_elapsed_ns) / 1000000000ULL;

        if (seconds == 0.0)
        {
            LOG_DEBUG(log, "{}, {} blocks, {} rows, {} bytes in 0 sec.",
                transform_message, stats.blocks, stats.rows, stats.bytes);
        }
        else
        {
            LOG_DEBUG(log, "{}, {} blocks, {} rows, {} bytes in {} sec., {} rows/sec., {}/sec.",
                transform_message, stats.blocks, stats.rows, stats.bytes,
                seconds, stats.rows / seconds, ReadableSize(stats.bytes / seconds));
        }
    }

private:
    using IMergingTransformBase::state;
};

using MergingTransformPtr = std::shared_ptr<IMergingTransformBase>;

}
