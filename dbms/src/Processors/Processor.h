#pragma once

#include <list>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <boost/noncopyable.hpp>
#include <Poco/Event.h>
#include <Core/Block.h>


/** Processor is an element of query execution pipeline.
  * It has zero or more input ports and zero or more output ports.
  *
  * Blocks of data are transferred over ports.
  * Each port has fixed structure: names and types of columns and values of constants.
  *
  * Processors may pull data from input ports, do some processing and push data to output ports.
  * Processor may indicate that it requires input data to proceed and set priorities of input ports.
  *
  * Synchronous work must only use CPU - don't do any sleep, IO wait, network wait.
  *
  * Processor may want to do work asynchronously (example: fetch data from remote server)
  *  - in this case it will initiate background job and allow to subscribe to it.
  *
  * Processor may throw an exception to indicate some runtime error.
  *
  * Different ports may have different structure. For example, ports may correspond to different datasets.
  *
  * TODO Ports may carry algebraic properties about streams of data.
  * For example, that data comes ordered by specific key; or grouped by specific key; or have unique values of specific key.
  *
  * Examples:
  *
  * Source. Has no input ports and single output port. Generates data itself and pushes it to its output port.
  *
  * Sink. Has single input port and no output ports. Consumes data that was passed to its input port.
  *
  * Empty source. Immediately says that data on its output port is finished.
  *
  * Null sink. Consumes data and does nothing.
  *
  * Simple transformation. Has single input and single output port. Pulls data, transforms it and pushes to output port.
  * Example: expression calculator.
  *
  * Squashing or filtering transformation. Pulls data, possibly accumulates it, and sometimes pushes it to output port.
  * Examples: DISTINCT, WHERE, squashing of blocks for INSERT SELECT.
  *
  * Accumulating transformation. Pulls and accumulates all data from input until it it exhausted, then pushes data to output port.
  * Examples: ORDER BY, GROUP BY.
  *
  * Limiting transformation. Pulls data from input and passes to output.
  * When there was enough data, says that it doesn't need data on its input and that data on its output port is finished.
  *
  * Resize. Has arbitary number of inputs and arbitary number of outputs. Pulls data from whatever ready input and pushes it to randomly choosed output.
  * Examples:
  * Union - merge data from number of inputs to one output in arbitary order.
  * Split - read data from one input and pass it to arbitary output.
  *
  * Concat. Has many inputs but only one output. Pulls all data from first input, then all data from second input, etc. and pushes it to output.
  *
  * Ordered merge. Has many inputs but only one output. Pulls data from selected input in specific order, merges and pushes it to output.
  */

namespace DB
{

class InputPort;
class OutputPort;
class IProcessor;


class Port
{
protected:
    friend class IProcessor;

    /// Shared state of two connected ports.
    struct State
    {
        Block data;
        bool needed = false;
        bool finished = false;
    };

    Block header;
    std::shared_ptr<State> state;

    IProcessor * processor = nullptr;

public:
    Port(const Block & header)
        : header(header) {}

    Block getHeader() const { return header; }
    bool isConnected() const { return state != nullptr; }

    void assumeConnected() const
    {
        if (!isConnected())
            throw Exception("Port is not connected");
    }

    bool hasData() const
    {
        assumeConnected();
        return state->data;
    }

    bool isNeeded() const
    {
        assumeConnected();
        return state->needed;
    }

    IProcessor & getProcessor()
    {
        if (!processor)
            throw Exception("Port does not belong to Processor");
        return *processor;
    }
};


class InputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    OutputPort * output_port = nullptr;

public:
    using Port::Port;

    Block pull()
    {
        if (!hasData())
            throw Exception("Port has no data");

        return std::move(state->data);
    }

    bool isFinished() const
    {
        assumeConnected();
        return state->finished;
    }

    void setNeeded()
    {
        assumeConnected();
        state->needed = true;
    }

    void setNotNeeded()
    {
        assumeConnected();
        state->needed = false;
    }

    OutputPort & getOutputPort()
    {
        assumeConnected();
        return *output_port;
    }
};


class OutputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    InputPort * input_port = nullptr;

public:
    using Port::Port;

    void push(Block && block)
    {
        if (hasData())
            throw Exception("Port already has data");

        state->data = std::move(block);
    }

    void setFinished()
    {
        assumeConnected();
        state->finished = true;
    }

    InputPort & getInputPort()
    {
        assumeConnected();
        return *input_port;
    }
};


using InputPorts = std::vector<InputPort>;
using OutputPorts = std::vector<OutputPort>;


inline void connect(OutputPort & output, InputPort & input)
{
    input.output_port = &output;
    output.input_port = &input;
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}


/** Allow to subscribe for multiple events and wait for them one by one in arbitary order.
  */
class EventCounter
{
private:
    size_t events_happened = 0;
    size_t events_waited = 0;

    mutable std::mutex mutex;
    std::condition_variable condvar;

public:
    void notify()
    {
        {
            std::lock_guard lock(mutex);
            ++events_happened;
        }
        condvar.notify_all();
    }

    void wait()
    {
        std::unique_lock lock(mutex);
        condvar.wait(lock, [&]{ return events_happened > events_waited; });
        ++events_waited;
    }

    template <typename Duration>
    bool waitFor(Duration && duration)
    {
        std::unique_lock lock(mutex);
        if (condvar.wait(lock, std::forward<Duration>(duration), [&]{ return events_happened > events_waited; }))
        {
            ++events_waited;
            return true;
        }
        return false;
    }
};


class IProcessor
{
protected:
    InputPorts inputs;
    OutputPorts outputs;

public:
    IProcessor() {}

    IProcessor(InputPorts && inputs_, OutputPorts && outputs_)
        : inputs(std::move(inputs_)), outputs(std::move(outputs_))
    {
        for (auto & port : inputs)
            port.processor = this;
        for (auto & port : outputs)
            port.processor = this;
    }

    virtual String getName() const = 0;

    enum class Status
    {
        /// Processor needs some data at its inputs to proceed.
        /// You need to run another processor to generate required input and proceed.
        NeedData,

        /// Processor cannot proceed because output port is full.
        /// You need to transfer data from output port to the input port of another processor.
        PortFull,

        /// All work is done, nothing more to do.
        Finished,

        /// No one needs data on output ports.
        Unneeded,

        /// You may call 'work' method and processor will do some work synchronously.
        Ready,

        /// You may call 'schedule' method and processor will initiate some background work.
        Async,

        /// Processor is doing some work in background and you have to wait.
        Wait
    };

    virtual Status prepare() = 0;

    /// You may call this method if 'status' returned Ready.
    virtual void work()
    {
        throw Exception("Method 'work' is not implemented for " + getName() + " processor");
    }

    /// You may call this method if 'status' returned Async.
    virtual void schedule(EventCounter & /*watch*/)
    {
        throw Exception("Method 'schedule' is not implemented for " + getName() + " processor");
    }

    virtual ~IProcessor() {}

    auto & getInputs() { return inputs; }
    auto & getOutputs() { return outputs; }
};

using ProcessorPtr = std::shared_ptr<IProcessor>;


/** Wraps pipeline in a single processor.
  * This processor has no inputs and outputs and just executes the pipeline,
  *  performing all synchronous work from the current thread.
  */
class SequentialPipelineExecutor : IProcessor
{
private:
    std::list<ProcessorPtr> processors;

    /// Look for first Ready or Async processor by depth-first search in needed input ports.
    /// NOTE: Pipeline must not have cycles.
    template <typename Visit, typename Finish>
    void traverse(IProcessor & processor, Visit && visit, Finish && finish)
    {
        Status status = processor.prepare();
        visit(processor, status);

        if (status == Status::Ready || status == Status::Async)
            return finish(processor, status);

        if (status == Status::NeedData)
            for (auto & input : processor.getInputs())
                if (input.isNeeded())
                    traverse(input.getOutputPort().getProcessor(), std::forward<Visit>(visit), std::forward<Finish>(finish));
    }

public:
    SequentialPipelineExecutor(const std::list<ProcessorPtr> & processors)
        : processors(processors)
    {
    }

    String getName() const override { return "SequentialPipelineExecutor"; }

    Status prepare() override
    {
        bool has_someone_to_wait = false;
        bool found = false;
        Status found_status = Status::Finished;

        for (auto & element : processors)
        {
            traverse(*element,
                [&] (IProcessor &, Status status)
                {
                    if (status == Status::Wait)
                        has_someone_to_wait = true;
                },
                [&] (IProcessor &, Status status)
                {
                    found = true;
                    found_status = status;
                });

            if (found)
                break;
        }

        if (found)
            return found_status;
        if (has_someone_to_wait)
            return Status::Wait;

        for (auto & element : processors)
        {
            if (element->prepare() == Status::NeedData)
                throw Exception("Pipeline stuck: " + element->getName() + " processor needs input data but no one is going to generate it");
            if (element->prepare() == Status::PortFull)
                throw Exception("Pipeline stuck: " + element->getName() + " processor has data in output port but no one is going to consume it");
        }

        return Status::Finished;
    }

    void work() override
    {
        bool found = false;
        for (auto & element : processors)
        {
            traverse(*element,
                [] (IProcessor &, Status) {},
                [&found] (IProcessor & processor, Status status)
                {
                    if (status == Status::Ready)
                    {
                        found = true;
                        //std::cerr << processor.getName() << " will work\n";
                        processor.work();
                    }
                });

            if (found)
                break;
        }

        if (!found)
            throw Exception("Bad pipeline");
    }

    void schedule(EventCounter & watch) override
    {
        bool found = false;
        for (auto & element : processors)
        {
            traverse(*element,
                [] (IProcessor &, Status) {},
                [&found, &watch] (IProcessor & processor, Status status)
                {
                    if (status == Status::Async)
                    {
                        found = true;
                        //std::cerr << processor.getName() << " will schedule\n";
                        processor.schedule(watch);
                    }
                });

            if (found)
                break;
        }

        if (!found)
            throw Exception("Bad pipeline");
    }
};


class ISource : public IProcessor
{
protected:
    OutputPort & output;
    bool finished = false;

    virtual Block generate() = 0;

public:
    ISource(Block && header)
        : IProcessor({}, {std::move(header)}), output(outputs.front())
    {
    }

    Status prepare() override
    {
        if (finished)
            return Status::Finished;

        if (output.hasData())
            return Status::PortFull;

        if (!output.isNeeded())
            return Status::Unneeded;

        return Status::Ready;
    }

    void work() override
    {
        if (Block block = generate())
            output.push(std::move(block));
        else
            finished = true;
    }

    OutputPort & getPort() { return output; }
};


class ISink : public IProcessor
{
protected:
    InputPort & input;

    virtual void consume(Block && block) = 0;

public:
    ISink(Block && header)
        : IProcessor({std::move(header)}, {}), input(inputs.front())
    {
    }

    Status prepare() override
    {
        if (input.hasData())
            return Status::Ready;

        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();
        return Status::NeedData;
    }

    void work() override
    {
        consume(input.pull());
    }

    InputPort & getPort() { return input; }
};


/** Has one input and one output.
  * Simply pull a block from input, transform it, and push it to output.
  */
class ITransform : public IProcessor
{
protected:
    InputPort & input;
    OutputPort & output;

    virtual void transform(Block & block) = 0;

public:
    ITransform(Block && input_header, Block && output_header)
        : IProcessor({std::move(input_header)}, {std::move(output_header)}),
        input(inputs.front()), output(outputs.front())
    {
    }

    Status prepare() override
    {
        if (output.hasData())
            return Status::PortFull;

        if (!output.isNeeded())
            return Status::Unneeded;

        if (input.hasData())
            return Status::Ready;

        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();
        return Status::NeedData;
    }

    void work() override
    {
        Block data = input.pull();
        transform(data);
        output.push(std::move(data));
    }

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
};


class ResizeProcessor : public IProcessor
{
public:
    using IProcessor::IProcessor;

    String getName() const override { return "Resize"; }

    Status prepare() override
    {
        bool all_outputs_full = true;
        bool all_outputs_unneeded = true;

        for (const auto & output : outputs)
        {
            if (!output.hasData())
                all_outputs_full = false;

            if (output.isNeeded())
                all_outputs_unneeded = false;
        }

        if (all_outputs_full)
            return Status::PortFull;

        if (all_outputs_unneeded)
        {
            for (auto & input : inputs)
                input.setNotNeeded();

            return Status::Unneeded;
        }

        bool all_inputs_finished = true;
        bool all_inputs_have_no_data = true;

        for (auto & input : inputs)
        {
            if (!input.isFinished())
            {
                all_inputs_finished = false;

                input.setNeeded();
                if (input.hasData())
                    all_inputs_have_no_data = false;
            }
        }

        if (all_inputs_finished)
            return Status::Finished;

        if (all_inputs_have_no_data)
            return Status::NeedData;

        return Status::Ready;
    }

    void work() override
    {
        for (auto & input : inputs)
        {
            if (input.hasData())
            {
                for (auto & output : outputs)
                {
                    if (!output.hasData())
                    {
                        output.push(input.pull());
                        break;
                    }
                }
                break;
            }
        }
    }
};


/*class AsynchronousProcessor : public IProcessor
{

};*/


class LimitTransform : public IProcessor
{
private:
    InputPort & input;
    OutputPort & output;

    size_t limit;
    size_t offset;
    size_t pos = 0; /// how many rows were read, including the last read block
    bool always_read_till_end;

public:
    LimitTransform(Block && header, size_t limit, size_t offset, bool always_read_till_end = false)
        : IProcessor({std::move(header)}, {std::move(header)}),
        input(inputs.front()), output(outputs.front()),
        limit(limit), offset(offset), always_read_till_end(always_read_till_end)
    {
    }

    String getName() const override { return "Limit"; }

    Status prepare() override
    {
        if (pos >= offset + limit)
        {
            output.setFinished();
            if (!always_read_till_end)
            {
                input.setNotNeeded();
                return Status::Finished;
            }
        }

        if (output.hasData())
            return Status::PortFull;

        if (!output.isNeeded())
            return Status::Unneeded;

        input.setNeeded();
        return input.hasData()
            ? Status::Ready
            : Status::NeedData;
    }

    void work() override
    {
        Block block = input.pull();

        if (pos >= offset + limit)
            return;

        size_t rows = block.rows();
        pos += rows;

        if (pos <= offset)
            return;

        /// return the whole block
        if (pos >= offset + rows && pos <= offset + limit)
        {
            output.push(std::move(block));
            return;
        }

        /// return a piece of the block
        size_t start = std::max(
            static_cast<Int64>(0),
            static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

        size_t length = std::min(
            static_cast<Int64>(limit), std::min(
            static_cast<Int64>(pos) - static_cast<Int64>(offset),
            static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows)));

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->cut(start, length);

        output.push(std::move(block));
    }

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
};

}
