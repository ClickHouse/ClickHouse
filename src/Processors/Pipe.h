#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Sources/SourceWithProgress.h>

namespace DB
{

class Pipe;
using Pipes = std::vector<Pipe>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

using OutputPortRawPtrs = std::vector<OutputPort *>;

/// Pipes is a set of processors which represents the part of pipeline.
/// Pipes contains a list of output ports, with specified port for totals and specified port for extremes.
/// All output ports have same header.
/// All other ports are connected, all connections are inside processors set.
class Pipe
{
public:
    /// Default constructor creates empty pipe. Generally, you cannot do anything with it except to check it is empty().
    /// You cannot get empty pipe in any other way. All transforms check that result pipe is not empty.
    Pipe() = default;
    /// Create from source. Source must have no input ports and single output.
    explicit Pipe(ProcessorPtr source);
    /// Create from processors. Use all not-connected output ports as output_ports. Check invariants.
    explicit Pipe(Processors processors_);

    Pipe(const Pipe & other) = delete;
    Pipe(Pipe && other) = default;
    Pipe & operator=(const Pipe & other) = delete;
    Pipe & operator=(Pipe && other) = default;

    const Block & getHeader() const { return header; }
    bool empty() const { return output_ports.empty(); }
    size_t numOutputPorts() const { return output_ports.size(); }
    OutputPort * getOutputPort(size_t pos) const { return output_ports[pos]; }
    OutputPort * getTotalsPort() const { return totals_port; }
    OutputPort * getExtremesPort() const { return extremes_port; }

    /// Add processors form other pipe. It should have same header.
    //void addPipes(Pipes pipes);

    /// Add processor to list, add it output ports to output_ports.
    /// Processor shouldn't have input ports, output ports shouldn't be connected.
    /// Output headers should have same structure and be compatible with current header (if not empty()).
    /// void addSource(ProcessorPtr source);

    /// Add totals and extremes.
    void addTotalsSource(ProcessorPtr source);
    void addExtremesSource(ProcessorPtr source);

    /// Add processor to list. It should have size() input ports with compatible header.
    /// Output ports should have same headers.
    /// If totals or extremes are not empty, transform shouldn't change header.
    void addTransform(ProcessorPtr transform);

    enum class StreamType
    {
        Main = 0, /// Stream for query data. There may be several streams of this type.
        Totals,  /// Stream for totals. No more then one.
        Extremes, /// Stream for extremes. No more then one.
    };

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header, StreamType stream_type)>;

    /// Add transform with single input and single output for each port.
    void addSimpleTransform(const ProcessorGetter & port);

    using Transformer = std::function<Processors(OutputPortRawPtrs ports)>;

    /// Transform Pipe in general way.
    void transform(const Transformer & transformer);

    /// Unite several pipes together. They should have same header.
    static Pipe unitePipes(Pipes pipes);

private:
    Processors processors;

    /// Header is common for all output below.
    Block header;

    /// Output ports. Totals and extremes are allowed to be empty.
    OutputPortRawPtrs output_ports;
    OutputPort * totals_port = nullptr;
    OutputPort * extremes_port = nullptr;

    /// It is the max number of processors which can be executed in parallel for each step. See QueryPipeline::Streams.
    /// Usually, it's the same as the number of output ports.
    size_t max_parallel_streams = 0;

    std::vector<TableLockHolder> table_locks;

    /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;

    /// Destroy pipes and get processors.
    static Processors detachProcessors(Pipe pipe) { return std::move(pipe.processors); }
};

/*
/// Pipe is a set of processors which represents the part of pipeline with single output.
/// All processors in pipe are connected. All ports are connected except the output one.
class Pipe
{
public:
    /// Create from source. It must have no input ports and single output.
    explicit Pipe(ProcessorPtr source);
    /// Connect several pipes together with specified transform.
    /// Transform must have the number of inputs equals to the number of pipes. And single output.
    /// Will connect pipes outputs with transform inputs automatically.
    Pipe(Pipes && pipes, ProcessorPtr transform);
    /// Create pipe from output port. If pipe was created that way, it possibly will not have tree shape.
    explicit Pipe(OutputPort * port);

    Pipe(const Pipe & other) = delete;
    Pipe(Pipe && other) = default;

    Pipe & operator=(const Pipe & other) = delete;
    Pipe & operator=(Pipe && other) = default;

    /// Append processors to pipe. After this, it possibly will not have tree shape.
    void addProcessors(const Processors & processors_);

    OutputPort & getPort() const { return *output_port; }
    const Block & getHeader() const { return output_port->getHeader(); }

    /// Add transform to pipe. It must have single input and single output (is checked).
    /// Input will be connected with current output port, output port will be updated.
    void addSimpleTransform(ProcessorPtr transform);

    Processors detachProcessors() && { return std::move(processors); }

    /// Specify quotas and limits for every ISourceWithProgress.
    void setLimits(const SourceWithProgress::LocalLimits & limits);
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota);

    /// Set information about preferred executor number for sources.
    void pinSources(size_t executor_number);

    void enableQuota();

    /// Totals and extremes port.
    void setTotalsPort(OutputPort * totals_) { totals = totals_; }
    void setExtremesPort(OutputPort * extremes_) { extremes = extremes_; }
    OutputPort * getTotalsPort() const { return totals; }
    OutputPort * getExtremesPort() const { return extremes; }

    size_t maxParallelStreams() const { return max_parallel_streams; }

    /// Do not allow to change the table while the processors of pipe are alive.
    /// TODO: move it to pipeline.
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }
    /// This methods are from QueryPipeline. Needed to make conversion from pipeline to pipe possible.
    void addInterpreterContext(std::shared_ptr<Context> context) { interpreter_context.emplace_back(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { storage_holders.emplace_back(std::move(storage)); }

    const std::vector<TableLockHolder> & getTableLocks() const { return table_locks; }
    const std::vector<std::shared_ptr<Context>> & getContexts() const { return interpreter_context; }
    const std::vector<StoragePtr> & getStorageHolders() const { return storage_holders; }

private:
    Processors processors;
    OutputPort * output_port = nullptr;
    OutputPort * totals = nullptr;
    OutputPort * extremes = nullptr;

    /// It is the max number of processors which can be executed in parallel for each step. See QueryPipeline::Streams.
    size_t max_parallel_streams = 0;

    std::vector<TableLockHolder> table_locks;

    /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;

    /// This private constructor is used only from QueryPipeline.
    /// It is not public, because QueryPipeline checks that processors are connected and have single output,
    ///  and therefore we can skip those checks.
    /// Note that Pipe represents a tree if it was created using public interface. But this constructor can't assert it.
    /// So, it's possible that TreeExecutorBlockInputStream could be unable to convert such Pipe to IBlockInputStream.
    explicit Pipe(Processors processors_, OutputPort * output_port, OutputPort * totals, OutputPort * extremes);

    friend class QueryPipeline;
};
*/
}
