#pragma once

#include <memory>
#include <Processors/Port.h>
#include <Common/Stopwatch.h>


class EventCounter;


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

class IQueryPlanStep;

struct StorageLimits;
using StorageLimitsList = std::list<StorageLimits>;

class RowsBeforeStepCounter;
using RowsBeforeStepCounterPtr = std::shared_ptr<RowsBeforeStepCounter>;

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
  * Resize. Has arbitrary number of inputs and arbitrary number of outputs.
  * Pulls data from whatever ready input and pushes it to randomly chosen free output.
  * Examples:
  * Union - merge data from number of inputs to one output in arbitrary order.
  * Split - read data from one input and pass it to arbitrary output.
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
  * Another example: we should support the notion of "arbitrary N-th of M substream" of full stream of data.
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

    enum class Status : uint8_t
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

        /// You may call 'schedule' method and processor will return descriptor.
        /// You need to poll this descriptor and call work() afterwards.
        Async,

        /// Processor wants to add other processors to pipeline.
        /// New processors must be obtained by expandPipeline() call.
        ExpandPipeline,
    };

    static std::string statusToName(std::optional<Status> status);

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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'prepare' is not implemented for {} processor", getName());
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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'work' is not implemented for {} processor", getName());
    }

    /** Executor must call this method when 'prepare' returned Async.
      * This method cannot access any ports. It should use only data that was prepared by 'prepare' method.
      *
      * This method should instantly return epollable file descriptor which will be readable when asynchronous job is done.
      * When descriptor is readable, method `work` is called to continue data processing.
      *
      * NOTE: it would be more logical to let `work()` return ASYNC status instead of prepare. This will get
      * prepare() -> work() -> schedule() -> work() -> schedule() -> .. -> work() -> prepare()
      * chain instead of
      * prepare() -> work() -> prepare() -> schedule() -> work() -> prepare() -> schedule() -> .. -> work() -> prepare()
      *
      * It is expected that executor epoll using level-triggered notifications.
      * Read all available data from descriptor before returning ASYNC.
      */
    virtual int schedule()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'schedule' is not implemented for {} processor", getName());
    }

    /* The method is called right after asynchronous job is done
     * i.e. when file descriptor returned by schedule() is readable.
     * The sequence of method calls:
     * ... prepare() -> schedule() -> onAsyncJobReady() -> work() ...
     * See also comment to schedule() method
     *
     * It allows doing some preprocessing immediately after asynchronous job is done.
     * The implementation should return control quickly, to avoid blocking another asynchronous completed jobs
     * created by the same pipeline.
     *
     * Example, scheduling tasks for remote workers (file descriptor in this case is a socket)
     * When the remote worker asks for the next task, doing it in onAsyncJobReady() we can provide it immediately.
     * Otherwise, the returning of the next task for the remote worker can be delayed by current work done in the pipeline
     * (by other processors), which will create unnecessary latency in query processing by remote workers
     */
    virtual void onAsyncJobReady() {}

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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'expandPipeline' is not implemented for {} processor", getName());
    }

    /// In case if query was cancelled executor will wait till all processors finish their jobs.
    /// Generally, there is no reason to check this flag. However, it may be reasonable for long operations (e.g. i/o).
    bool isCancelled() const { return is_cancelled.load(std::memory_order_acquire); }
    void cancel() noexcept;

    /// Additional method which is called in case if ports were updated while work() method.
    /// May be used to stop execution in rare cases.
    virtual void onUpdatePorts() {}

    virtual ~IProcessor() = default;

    auto & getInputs() { return inputs; }
    auto & getOutputs() { return outputs; }

    UInt64 getInputPortNumber(const InputPort * input_port) const
    {
        UInt64 number = 0;
        for (const auto & port : inputs)
        {
            if (&port == input_port)
                return number;

            ++number;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find input port for {} processor", getName());
    }

    UInt64 getOutputPortNumber(const OutputPort * output_port) const
    {
        UInt64 number = 0;
        for (const auto & port : outputs)
        {
            if (&port == output_port)
                return number;

            ++number;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find output port for {} processor", getName());
    }

    const auto & getInputs() const { return inputs; }
    const auto & getOutputs() const { return outputs; }

    /// Debug output.
    String debug() const;
    void dump() const;

    /// Used to print pipeline.
    void setDescription(const std::string & description_) { processor_description = description_; }
    const std::string & getDescription() const { return processor_description; }

    /// Helpers for pipeline executor.
    void setStream(size_t value) { stream_number = value; }
    size_t getStream() const { return stream_number; }
    constexpr static size_t NO_STREAM = std::numeric_limits<size_t>::max();

    /// Step of QueryPlan from which processor was created.
    void setQueryPlanStep(IQueryPlanStep * step, size_t group = 0);

    IQueryPlanStep * getQueryPlanStep() const { return query_plan_step; }
    size_t getQueryPlanStepGroup() const { return query_plan_step_group; }
    const String & getPlanStepName() const { return plan_step_name; }
    const String & getPlanStepDescription() const { return plan_step_description; }

    uint64_t getElapsedNs() const { return elapsed_ns; }
    uint64_t getInputWaitElapsedNs() const { return input_wait_elapsed_ns; }
    uint64_t getOutputWaitElapsedNs() const { return output_wait_elapsed_ns; }

    struct ProcessorDataStats
    {
        size_t input_rows = 0;
        size_t input_bytes = 0;
        size_t output_rows = 0;
        size_t output_bytes = 0;
    };

    ProcessorDataStats getProcessorDataStats() const
    {
        ProcessorDataStats stats;

        for (const auto & input : inputs)
        {
            stats.input_rows += input.rows;
            stats.input_bytes += input.bytes;
        }

        for (const auto & output : outputs)
        {
            stats.output_rows += output.rows;
            stats.output_bytes += output.bytes;
        }

        return stats;
    }

    struct ReadProgressCounters
    {
        uint64_t read_rows = 0;
        uint64_t read_bytes = 0;
        uint64_t total_rows_approx = 0;
        uint64_t total_bytes = 0;
    };

    struct ReadProgress
    {
        ReadProgressCounters counters;
        const StorageLimitsList & limits;
    };

    /// Set limits for current storage.
    /// Different limits may be applied to different storages, we need to keep it per processor.
    /// This method needs to be overridden only for sources.
    virtual void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & /*storage_limits*/) {}

    /// This method is called for every processor without input ports.
    /// Processor can return new progress for the last read operation.
    /// You should zero internal counters in the call, in order to make in idempotent.
    virtual std::optional<ReadProgress> getReadProgress() { return std::nullopt; }

    /// Set rows_before_limit counter for current processor.
    /// This counter is used to calculate the number of rows right before any filtration of LimitTransform.
    virtual void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr /* counter */) { }

    /// Set rows_before_aggregation counter for current processor.
    /// This counter is used to calculate the number of rows right before AggregatingTransform.
    virtual void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr /* counter */) { }

protected:
    virtual void onCancel() noexcept {}

    std::atomic<bool> is_cancelled{false};

private:
    /// For:
    /// - elapsed_ns
    friend class ExecutionThreadContext;
    /// For
    /// - input_wait_elapsed_ns
    /// - output_wait_elapsed_ns
    friend class ExecutingGraph;

    std::string processor_description;

    /// For processors_profile_log
    uint64_t elapsed_ns = 0;
    Stopwatch input_wait_watch;
    uint64_t input_wait_elapsed_ns = 0;
    Stopwatch output_wait_watch;
    uint64_t output_wait_elapsed_ns = 0;

    size_t stream_number = NO_STREAM;

    IQueryPlanStep * query_plan_step = nullptr;
    size_t query_plan_step_group = 0;
    String plan_step_name;
    String plan_step_description;
};


}
