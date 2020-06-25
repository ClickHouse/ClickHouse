#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

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
        bool have_all_inputs_);

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
        Chunk input_chunk;
        bool is_finished = false;
        bool need_data = false;
        size_t next_input_to_read = 0;

        Chunks init_chunks;
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
        Args && ... args)
        : IMergingTransformBase(num_inputs, input_header, output_header, have_all_inputs_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    void work() override
    {
        if (!state.init_chunks.empty())
            algorithm.initialize(std::move(state.init_chunks));

        if (state.input_chunk)
        {
            // std::cerr << "Consume chunk with " << state.input_chunk.getNumRows()
            //           << " for input " << state.next_input_to_read << std::endl;
            algorithm.consume(std::move(state.input_chunk), state.next_input_to_read);
        }

        IMergingAlgorithm::Status status = algorithm.merge();

        if (status.chunk && status.chunk.hasRows())
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
    }

protected:
    Algorithm algorithm;

    /// Profile info.
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

private:
    using IMergingTransformBase::state;
};

}
