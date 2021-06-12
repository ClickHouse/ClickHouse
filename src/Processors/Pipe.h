#pragma once

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/QueryIdHolder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Access/EnabledQuota.h>
#include <DataStreams/SizeLimits.h>
#include <Storages/TableLockHolder.h>

namespace DB
{

struct StreamLocalLimits;

class Pipe;
using Pipes = std::vector<Pipe>;

class QueryPipeline;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

using OutputPortRawPtrs = std::vector<OutputPort *>;

/// Pipe is a set of processors which represents the part of pipeline.
/// Pipe contains a list of output ports, with specified port for totals and specified port for extremes.
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
    /// Create from source with specified totals end extremes (may be nullptr). Ports should be owned by source.
    explicit Pipe(ProcessorPtr source, OutputPort * output, OutputPort * totals, OutputPort * extremes);
    /// Create from processors. Use all not-connected output ports as output_ports. Check invariants.
    explicit Pipe(Processors processors_);

    Pipe(const Pipe & other) = delete;
    Pipe(Pipe && other) = default;
    Pipe & operator=(const Pipe & other) = delete;
    Pipe & operator=(Pipe && other) = default;

    const Block & getHeader() const { return header; }
    bool empty() const { return processors.empty(); }
    size_t numOutputPorts() const { return output_ports.size(); }
    size_t maxParallelStreams() const { return max_parallel_streams; }
    OutputPort * getOutputPort(size_t pos) const { return output_ports[pos]; }
    OutputPort * getTotalsPort() const { return totals_port; }
    OutputPort * getExtremesPort() const { return extremes_port; }

    /// Add processor to list, add it output ports to output_ports.
    /// Processor shouldn't have input ports, output ports shouldn't be connected.
    /// Output headers should have same structure and be compatible with current header (if not empty()).
    void addSource(ProcessorPtr source);

    /// Add totals and extremes.
    void addTotalsSource(ProcessorPtr source);
    void addExtremesSource(ProcessorPtr source);

    /// Drop totals and extremes (create NullSink for them).
    void dropTotals();
    void dropExtremes();

    /// Add processor to list. It should have size() input ports with compatible header.
    /// Output ports should have same headers.
    /// If totals or extremes are not empty, transform shouldn't change header.
    void addTransform(ProcessorPtr transform);
    void addTransform(ProcessorPtr transform, OutputPort * totals, OutputPort * extremes);
    void addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes);

    enum class StreamType
    {
        Main = 0, /// Stream for query data. There may be several streams of this type.
        Totals,  /// Stream for totals. No more than one.
        Extremes, /// Stream for extremes. No more than one.
    };

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header)>;
    using ProcessorGetterWithStreamKind = std::function<ProcessorPtr(const Block & header, StreamType stream_type)>;

    /// Add transform with single input and single output for each port.
    void addSimpleTransform(const ProcessorGetter & getter);
    void addSimpleTransform(const ProcessorGetterWithStreamKind & getter);

    /// Changes the number of output ports if needed. Adds ResizeTransform.
    void resize(size_t num_streams, bool force = false, bool strict = false);

    using Transformer = std::function<Processors(OutputPortRawPtrs ports)>;

    /// Transform Pipe in general way.
    void transform(const Transformer & transformer);

    /// Unite several pipes together. They should have same header.
    static Pipe unitePipes(Pipes pipes);

    /// Get processors from Pipe. Use it with cautious, it is easy to loss totals and extremes ports.
    static Processors detachProcessors(Pipe pipe) { return std::move(pipe.processors); }
    /// Get processors from Pipe w/o destroying pipe (used for EXPLAIN to keep QueryPlan).
    const Processors & getProcessors() const { return processors; }

    /// Specify quotas and limits for every ISourceWithProgress.
    void setLimits(const StreamLocalLimits & limits);
    void setLeafLimits(const SizeLimits & leaf_limits);
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota);

    /// Do not allow to change the table while the processors of pipe are alive.
    void addTableLock(TableLockHolder lock) { holder.table_locks.emplace_back(std::move(lock)); }
    /// This methods are from QueryPipeline. Needed to make conversion from pipeline to pipe possible.
    void addInterpreterContext(std::shared_ptr<const Context> context) { holder.interpreter_context.emplace_back(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { holder.storage_holders.emplace_back(std::move(storage)); }
    void addQueryIdHolder(std::shared_ptr<QueryIdHolder> query_id_holder) { holder.query_id_holder = std::move(query_id_holder); }
    /// For queries with nested interpreters (i.e. StorageDistributed)
    void addQueryPlan(std::unique_ptr<QueryPlan> plan) { holder.query_plans.emplace_back(std::move(plan)); }

private:
    /// Destruction order: processors, header, locks, temporary storages, local contexts

    struct Holder
    {
        Holder() = default;
        Holder(Holder &&) = default;
        /// Custom mode assignment does not destroy data from lhs. It appends data from rhs to lhs.
        Holder& operator=(Holder &&);

        /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
        /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
        /// because QueryPipeline is alive until query is finished.
        std::vector<std::shared_ptr<const Context>> interpreter_context;
        std::vector<StoragePtr> storage_holders;
        std::vector<TableLockHolder> table_locks;
        std::vector<std::unique_ptr<QueryPlan>> query_plans;
        std::shared_ptr<QueryIdHolder> query_id_holder;
    };

    Holder holder;

    /// Header is common for all output below.
    Block header;
    Processors processors;

    /// Output ports. Totals and extremes are allowed to be empty.
    OutputPortRawPtrs output_ports;
    OutputPort * totals_port = nullptr;
    OutputPort * extremes_port = nullptr;

    /// It is the max number of processors which can be executed in parallel for each step.
    /// Usually, it's the same as the number of output ports.
    size_t max_parallel_streams = 0;

    /// If is set, all newly created processors will be added to this too.
    /// It is needed for debug. See QueryPipelineProcessorsCollector.
    Processors * collected_processors = nullptr;

    /// This methods are for QueryPipeline. It is allowed to complete graph only there.
    /// So, we may be sure that Pipe always has output port if not empty.
    bool isCompleted() const { return !empty() && output_ports.empty(); }
    static Pipe unitePipes(Pipes pipes, Processors * collected_processors, bool allow_empty_header);
    void setSinks(const Pipe::ProcessorGetterWithStreamKind & getter);
    void setOutputFormat(ProcessorPtr output);

    friend class QueryPipeline;
};

}
