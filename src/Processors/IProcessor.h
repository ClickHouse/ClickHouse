#pragma once

#include <memory>
#include <Processors/Port.h>


class EventCounter;


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

/** Processor is an element (low level building block) of a query execution pipeline.
  * It has zero or more input ports and zero or more output ports.
  *
  * Blocks of data are transferred over ports.
  * Each port has fixed structure: names and types of columns and values of constants.
  *
  * Processors may pull data from input ports, do some processing and push data to output ports.
  * Processor may indicate that it requires input data to proceed and indicate that it needs data from some ports.
  *
  * Synchronous work must only use CPU - don't do any sleep, IO wait, network wait.
  *
  * Processor may want to do work asynchronously (example: fetch data from remote server)
  *  - in this case it will initiate background job and allow to subscribe to it.
  *
  * Processor may throw an exception to indicate some runtime error.
  *
  * Different ports may have different structure. For example, ports may correspond to different resultsets
  *  or semantically different parts of result.
  *
  * Processor may modify its ports (create another processors and connect to them) on the fly.
  * Example: first execute the subquery; on basis of subquery result
  *  determine how to execute the rest of query and build the corresponding pipeline.
  *
  * Processor may simply wait for another processor to execute without transferring any data from it.
  * For this purpose it should connect its input port to another processor, and indicate need of data.
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
  * TODO Better to make each function a separate processor. It's better for pipeline analysis. Also keep in mind 'sleep' and 'rand' functions.
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
  * Resize. Has arbitary number of inputs and arbitary number of outputs.
  * Pulls data from whatever ready input and pushes it to randomly choosed free output.
  * Examples:
  * Union - merge data from number of inputs to one output in arbitary order.
  * Split - read data from one input and pass it to arbitary output.
  *
  * Concat. Has many inputs and only one output. Pulls all data from first input until it is exhausted,
  *  then all data from second input, etc. and pushes all data to output.
  *
  * Ordered merge. Has many inputs but only one output. Pulls data from selected input in specific order, merges and pushes it to output.
  *
  * Fork. Has one input and many outputs. Pulls data from input and copies it to all outputs.
  * Used to process multiple queries with common source of data.
  *
  * Select. Has one or multiple inputs and one output.
  * Read blocks from inputs and check that blocks on inputs are "parallel": correspond to each other in number of rows.
  * Construct a new block by selecting some subset (or all) of columns from inputs.
  * Example: collect columns - function arguments before function execution.
  *
  *
  * TODO Processors may carry algebraic properties about transformations they do.
  * For example, that processor doesn't change number of rows; doesn't change order of rows, doesn't change the set of rows, etc.
  *
  * TODO Ports may carry algebraic properties about streams of data.
  * For example, that data comes ordered by specific key; or grouped by specific key; or have unique values of specific key.
  * And also simple properties, including lower and upper bound on number of rows.
  *
  * TODO Processor should have declarative representation, that is able to be serialized and parsed.
  * Example: read_from_merge_tree(database, table, Columns(a, b, c), Piece(0, 10), Parts(Part('name', MarkRanges(MarkRange(0, 100), ...)), ...))
  * It's reasonable to have an intermediate language for declaration of pipelines.
  *
  * TODO Processor with all its parameters should represent "pure" function on streams of data from its input ports.
  * It's in question, what kind of "pure" function do we mean.
  * For example, data streams are considered equal up to order unless ordering properties are stated explicitly.
  * Another example: we should support the notion of "arbitary N-th of M substream" of full stream of data.
  */

class IProcessor
{
protected:
    InputPorts inputs;
    OutputPorts outputs;

public:
    IProcessor() = default;

    IProcessor(InputPorts inputs_, OutputPorts outputs_)
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
        /// You need to run another processor to generate required input and then call 'prepare' again.
        NeedData,

        /// Processor cannot proceed because output port is full or not isNeeded().
        /// You need to transfer data from output port to the input port of another processor and then call 'prepare' again.
        PortFull,

        /// All work is done (all data is processed or all output are closed), nothing more to do.
        Finished,

        /// No one needs data on output ports.
        /// Unneeded,

        /// You may call 'work' method and processor will do some work synchronously.
        Ready,

        /// You may call 'schedule' method and processor will initiate some background work.
        Async,

        /// Processor is doing some work in background.
        /// You may wait for next event or do something else and then you should call 'prepare' again.
        Wait,

        /// Processor wants to add other processors to pipeline.
        /// New processors must be obtained by expandPipeline() call.
        ExpandPipeline,
    };

    static std::string statusToName(Status status);

    /** Method 'prepare' is responsible for all cheap ("instantaneous": O(1) of data volume, no wait) calculations.
      *
      * It may access input and output ports,
      *  indicate the need for work by another processor by returning NeedData or PortFull,
      *  or indicate the absence of work by returning Finished or Unneeded,
      *  it may pull data from input ports and push data to output ports.
      *
      * The method is not thread-safe and must be called from a single thread in one moment of time,
      *  even for different connected processors.
      *
      * Instead of all long work (CPU calculations or waiting) it should just prepare all required data and return Ready or Async.
      *
      * Thread safety and parallel execution:
      * - no methods (prepare, work, schedule) of single object can be executed in parallel;
      * - method 'work' can be executed in parallel for different objects, even for connected processors;
      * - method 'prepare' cannot be executed in parallel even for different objects,
      *   if they are connected (including indirectly) to each other by their ports;
      */
    virtual Status prepare()
    {
        throw Exception("Method 'prepare' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
    }

    using PortNumbers = std::vector<UInt64>;

    /// Optimization for prepare in case we know ports were updated.
    virtual Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) { return prepare(); }

    /** You may call this method if 'prepare' returned Ready.
      * This method cannot access any ports. It should use only data that was prepared by 'prepare' method.
      *
      * Method work can be executed in parallel for different processors.
      */
    virtual void work()
    {
        throw Exception("Method 'work' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
    }

    /** You may call this method if 'prepare' returned Async.
      * This method cannot access any ports. It should use only data that was prepared by 'prepare' method.
      *
      * This method should return instantly and fire an event (or many events) when asynchronous job will be done.
      * When the job is not done, method 'prepare' will return Wait and the user may block and wait for next event before checking again.
      *
      * Note that it can fire many events in EventCounter while doing its job,
      *  and you have to wait for next event (or do something else) every time when 'prepare' returned Wait.
      */
    virtual void schedule(EventCounter & /*watch*/)
    {
        throw Exception("Method 'schedule' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
    }

    /** You must call this method if 'prepare' returned ExpandPipeline.
      * This method cannot access any port, but it can create new ports for current processor.
      *
      * Method should return set of new already connected processors.
      * All added processors must be connected only to each other or current processor.
      *
      * Method can't remove or reconnect existing ports, move data from/to port or perform calculations.
      * 'prepare' should be called again after expanding pipeline.
      */
    virtual Processors expandPipeline()
    {
        throw Exception("Method 'expandPipeline' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// In case if query was cancelled executor will wait till all processors finish their jobs.
    /// Generally, there is no reason to check this flag. However, it may be reasonable for long operations (e.g. i/o).
    bool isCancelled() const { return is_cancelled; }
    void cancel()
    {
        is_cancelled = true;
        onCancel();
    }

    /// Additional method which is called in case if ports were updated while work() method.
    /// May be used to stop execution in rare cases.
    virtual void onUpdatePorts() {}

    virtual ~IProcessor() = default;

    auto & getInputs() { return inputs; }
    auto & getOutputs() { return outputs; }

    UInt64 getInputPortNumber(const InputPort * input_port) const
    {
        UInt64 number = 0;
        for (auto & port : inputs)
        {
            if (&port == input_port)
                return number;

            ++number;
        }

        throw Exception("Can't find input port for " + getName() + " processor", ErrorCodes::LOGICAL_ERROR);
    }

    UInt64 getOutputPortNumber(const OutputPort * output_port) const
    {
        UInt64 number = 0;
        for (auto & port : outputs)
        {
            if (&port == output_port)
                return number;

            ++number;
        }

        throw Exception("Can't find output port for " + getName() + " processor", ErrorCodes::LOGICAL_ERROR);
    }

    const auto & getInputs() const { return inputs; }
    const auto & getOutputs() const { return outputs; }

    /// Debug output.
    void dump() const;

    /// Used to print pipeline.
    void setDescription(const std::string & description_) { processor_description = description_; }
    const std::string & getDescription() const { return processor_description; }

    /// Helpers for pipeline executor.
    void setStream(size_t value) { stream_number = value; }
    size_t getStream() const { return stream_number; }
    constexpr static size_t NO_STREAM = std::numeric_limits<size_t>::max();

    void enableQuota() { has_quota = true; }
    bool hasQuota() const { return has_quota; }

protected:
    virtual void onCancel() {}

private:
    std::atomic<bool> is_cancelled{false};

    std::string processor_description;

    size_t stream_number = NO_STREAM;

    bool has_quota = false;
};


}
