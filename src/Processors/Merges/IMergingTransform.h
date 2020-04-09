#pragma once

#include <Processors/Merges/IMergingAlgorithm.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

namespace DB
{

class MergedData;

/// Base class for merging transforms.
class IMergingTransform : public IProcessor
{
public:
    IMergingTransform(
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        //size_t max_block_size,
        //bool use_average_block_size,  /// For adaptive granularity. Return chunks with the same avg size as inputs.
        bool have_all_inputs_);

    /// Methods to add additional input port. It is possible to do only before the first call of `prepare`.
    void addInput();
    /// Need to be called after all inputs are added. (only if have_all_inputs was not specified).
    void setHaveAllInputs();

    Status prepare() override;

protected:

    virtual void onNewInput(); /// Is called when new input is added. To initialize input's data.
    virtual void initializeInputs() = 0; /// Is called after first chunk was read for every input.
    virtual void consume(Chunk chunk, size_t input_number) = 0; /// Is called after chunk was consumed from input.
    virtual void onFinish() {} /// Is called when all data is processed.

    void requestDataForInput(size_t input_number); /// Call it to say that next chunk of data is required for input.
    void prepareOutputChunk(MergedData & merged_data); /// Moves chunk from merged_data to output_chunk if needed.

    /// Profile info.
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    Chunk output_chunk;
    bool has_output_chunk = false;
    bool is_finished = false;

private:
    /// Processor state.
    bool is_initialized = false;
    bool need_data = false;
    size_t next_input_to_read = 0;

    std::atomic<bool> have_all_inputs;

    struct InputState
    {
        explicit InputState(InputPort & port_) : port(port_) {}

        InputPort & port;
        bool is_initialized = false;
    };

    std::vector<InputState> input_states;

    Status prepareInitializeInputs();
};

/// Base class for merging transforms.
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
    Chunk output_chunk;
    Chunk input_chunk;
    bool is_finished = false;
    bool need_data = false;
    size_t next_input_to_read = 0;

    Chunks init_chunks;

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

template <typename Algorithm>
class IMergingTransform2 : public IMergingTransformBase
{
public:
    IMergingTransform2(
        Algorithm algorithm_,
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        bool have_all_inputs_)
        : IMergingTransformBase(num_inputs, input_header, output_header, have_all_inputs_)
        , algorithm(std::move(algorithm_))
    {
    }

    void work() override
    {
        if (!init_chunks.empty())
            algorithm.initialize(std::move(init_chunks));

        if (input_chunk)
            algorithm.consume(std::move(input_chunk), next_input_to_read);

        IMergingAlgorithm::Status status = algorithm.merge();

        if (status.chunk && status.chunk.hasRows())
            output_chunk = std::move(status.chunk);

        if (status.required_source >= 0)
        {
            next_input_to_read = status.required_source;
            need_data = true;
        }

        if (status.is_finished)
            is_finished = true;
    }

protected:
    Algorithm algorithm;

    /// Profile info.
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

private:
    using IMergingTransformBase::output_chunk;
    using IMergingTransformBase::input_chunk;
    using IMergingTransformBase::is_finished;
    using IMergingTransformBase::need_data;
    using IMergingTransformBase::next_input_to_read;
    using IMergingTransformBase::init_chunks;
};

}
