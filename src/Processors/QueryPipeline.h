#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Pipe.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>

namespace DB
{


using TableLockHolders = std::vector<TableLockHolder>;
class Context;

class IOutputFormat;

class QueryPipelineProcessorsCollector;

class QueryPipeline
{
private:
    /// It's a wrapper over std::vector<OutputPort *>
    /// Is needed to support invariant for max_parallel_streams (see comment below).
    class Streams
    {
    public:
        auto size() const { return data.size(); }
        bool empty() const { return size() == 0; }
        auto begin() { return data.begin(); }
        auto end() { return data.end(); }
        auto & front() { return data.front(); }
        auto & back() { return data.back(); }
        auto & at(size_t pos) { return data.at(pos); }
        auto & operator[](size_t pos) { return data[pos]; }

        void clear() { data.clear(); }
        void reserve(size_t size_) { data.reserve(size_); }

        void addStream(OutputPort * port, size_t port_max_parallel_streams)
        {
            data.push_back(port);
            max_parallel_streams = std::max<size_t>(max_parallel_streams + port_max_parallel_streams, data.size());
        }

        void addStreams(Streams & other)
        {
            data.insert(data.end(), other.begin(), other.end());
            max_parallel_streams = std::max<size_t>(max_parallel_streams + other.max_parallel_streams, data.size());
        }

        void assign(std::initializer_list<OutputPort *> list)
        {
            data = list;
            max_parallel_streams = std::max<size_t>(max_parallel_streams, data.size());
        }

        size_t maxParallelStreams() const { return max_parallel_streams; }

    private:
        std::vector<OutputPort *> data;

        /// It is the max number of processors which can be executed in parallel for each step.
        /// Logically, it is the upper limit on the number of threads needed to execute this pipeline.
        /// Initially, it is the number of sources. It may be increased after resize, aggregation, etc.
        /// This number is never decreased, and it is calculated as max(streams.size()) over all streams while building.
        size_t max_parallel_streams = 0;
    };

public:

    class ProcessorsContainer
    {
    public:
        bool empty() const { return processors.empty(); }
        void emplace(ProcessorPtr processor);
        void emplace(Processors processors_);
        Processors * getCollectedProcessors() const { return collected_processors; }
        Processors * setCollectedProcessors(Processors * collected_processors);
        Processors & get() { return processors; }
        const Processors & get() const { return processors; }
        Processors detach() { return std::move(processors); }
    private:
        /// All added processors.
        Processors processors;

        /// If is set, all newly created processors will be added to this too.
        /// It is needed for debug. See QueryPipelineProcessorsCollector below.
        Processors * collected_processors = nullptr;
    };

    QueryPipeline() = default;
    QueryPipeline(QueryPipeline &&) = default;
    ~QueryPipeline() = default;
    QueryPipeline(const QueryPipeline &) = delete;
    QueryPipeline & operator= (const QueryPipeline & rhs) = delete;

    QueryPipeline & operator= (QueryPipeline && rhs);

    /// All pipes must have same header.
    void init(Pipes pipes);
    void init(Pipe pipe); /// Simple init for single pipe
    bool initialized() { return !processors.empty(); }
    bool isCompleted() { return initialized() && streams.empty(); }

    /// Type of logical data stream for simple transform.
    /// Sometimes it's important to know which part of pipeline we are working for.
    /// Example: ExpressionTransform need special logic for totals.
    enum class StreamType
    {
        Main = 0, /// Stream for query data. There may be several streams of this type.
        Totals,  /// Stream for totals. No more then one.
        Extremes, /// Stream for extremes. No more then one.
    };

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header)>;
    using ProcessorGetterWithStreamKind = std::function<ProcessorPtr(const Block & header, StreamType stream_type)>;

    /// Add transform with simple input and simple output for each port.
    void addSimpleTransform(const ProcessorGetter & getter);
    void addSimpleTransform(const ProcessorGetterWithStreamKind & getter);
    /// Add several processors. They must have same header for inputs and same for outputs.
    /// Total number of inputs must be the same as the number of streams. Output ports will become new streams.
    void addPipe(Processors pipe);
    /// Add TotalsHavingTransform. Resize pipeline to single input. Adds totals port.
    void addTotalsHavingTransform(ProcessorPtr transform);
    /// Add transform which calculates extremes. This transform adds extremes port and doesn't change inputs number.
    void addExtremesTransform();
    /// Adds transform which creates sets. It will be executed before reading any data from input ports.
    void addCreatingSetsTransform(ProcessorPtr transform);
    /// Resize pipeline to single output and add IOutputFormat. Pipeline will be completed after this transformation.
    void setOutputFormat(ProcessorPtr output);
    /// Get current OutputFormat.
    IOutputFormat * getOutputFormat() const { return output_format; }
    /// Sink is a processor with single input port and no output ports. Creates sink for each output port.
    /// Pipeline will be completed after this transformation.
    void setSinks(const ProcessorGetterWithStreamKind & getter);

    /// Add totals which returns one chunk with single row with defaults.
    void addDefaultTotals();

    /// Add already calculated totals.
    void addTotals(ProcessorPtr source);

    /// Forget about current totals and extremes. It is needed before aggregation, cause they will be calculated again.
    void dropTotalsAndExtremes();

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);

    /// Check if resize transform was used. (In that case another distinct transform will be added).
    bool hasMixedStreams() const { return has_resize || hasMoreThanOneStream(); }

    /// Changes the number of input ports if needed. Adds ResizeTransform.
    void resize(size_t num_streams, bool force = false, bool strict = false);

    void enableQuotaForCurrentStreams();

    /// Unite several pipelines together. Result pipeline would have common_header structure.
    /// If collector is used, it will collect only newly-added processors, but not processors from pipelines.
    void unitePipelines(std::vector<std::unique_ptr<QueryPipeline>> pipelines, const Block & common_header, size_t max_threads_limit = 0);

    PipelineExecutorPtr execute();

    size_t getNumStreams() const { return streams.size(); }

    bool hasMoreThanOneStream() const { return getNumStreams() > 1; }
    bool hasTotals() const { return totals_having_port != nullptr; }

    const Block & getHeader() const { return current_header; }

    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }
    void addInterpreterContext(std::shared_ptr<Context> context) { interpreter_context.emplace_back(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { storage_holders.emplace_back(std::move(storage)); }

    /// For compatibility with IBlockInputStream.
    void setProgressCallback(const ProgressCallback & callback);
    void setProcessListElement(QueryStatus * elem);

    /// Recommend number of threads for pipeline execution.
    size_t getNumThreads() const
    {
        auto num_threads = streams.maxParallelStreams();

        if (max_threads)
            num_threads = std::min(num_threads, max_threads);

        return std::max<size_t>(1, num_threads);
    }

    /// Set upper limit for the recommend number of threads
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }

    /// Update upper limit for the recommend number of threads
    void limitMaxThreads(size_t max_threads_)
    {
        if (max_threads == 0 || max_threads_ < max_threads)
            max_threads = max_threads_;
    }

    /// Convert query pipeline to single or several pipes.
    Pipe getPipe() &&;
    Pipes getPipes() &&;

    /// Get internal processors.
    const Processors & getProcessors() const { return processors.get(); }

private:
    /// Destruction order: processors, header, locks, temporary storages, local contexts

    /// Some Streams (or Processors) may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
    TableLockHolders table_locks;

    /// Common header for each stream.
    Block current_header;

    ProcessorsContainer processors;

    /// Port for each independent "stream".
    Streams streams;

    /// Special ports for extremes and totals having.
    OutputPort * totals_having_port = nullptr;
    OutputPort * extremes_port = nullptr;

    /// If resize processor was added to pipeline.
    bool has_resize = false;

    IOutputFormat * output_format = nullptr;

    /// Limit on the number of threads. Zero means no limit.
    /// Sometimes, more streams are created then the number of threads for more optimal execution.
    size_t max_threads = 0;

    QueryStatus * process_list_element = nullptr;

    void checkInitialized();
    void checkInitializedAndNotCompleted();
    static void checkSource(const ProcessorPtr & source, bool can_have_totals);

    template <typename TProcessorGetter>
    void addSimpleTransformImpl(const TProcessorGetter & getter);

    void initRowsBeforeLimit();

    friend class QueryPipelineProcessorsCollector;
};

/// This is a small class which collects newly added processors to QueryPipeline.
/// Pipeline must live longer that this class.
class QueryPipelineProcessorsCollector
{
public:
    explicit QueryPipelineProcessorsCollector(QueryPipeline & pipeline_, IQueryPlanStep * step_ = nullptr);
    ~QueryPipelineProcessorsCollector();

    Processors detachProcessors(size_t group = 0);

private:
    QueryPipeline & pipeline;
    IQueryPlanStep * step;
    Processors processors;
};

}
