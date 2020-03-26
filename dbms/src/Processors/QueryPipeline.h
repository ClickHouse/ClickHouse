#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Pipe.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Storages/IStorage_fwd.h>

namespace DB
{

class TableStructureReadLock;
using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockHolder>;

class Context;

class IOutputFormat;

class QueryPipeline
{
private:
    /// It's a wrapper over std::vector<OutputPort *>
    /// Is needed to support invariant for max_parallel_streams (see comment below).
    class Streams
    {
    public:
        auto size() const { return data.size(); }
        auto begin() { return data.begin(); }
        auto end() { return data.end(); }
        auto & front() { return data.front(); }
        auto & back() { return data.back(); }
        auto & at(size_t pos) { return data.at(pos); }
        auto & operator[](size_t pos) { return data[pos]; }

        void clear() { data.clear(); }
        void reserve(size_t size_) { data.reserve(size_); }

        void addStream(OutputPort * port)
        {
            data.push_back(port);
            max_parallel_streams = std::max<size_t>(max_parallel_streams, data.size());
        }

        void addStreams(Streams & other)
        {
            data.insert(data.end(), other.begin(), other.end());
            max_parallel_streams = std::max<size_t>(max_parallel_streams, data.size());
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

    void addSimpleTransform(const ProcessorGetter & getter);
    void addSimpleTransform(const ProcessorGetterWithStreamKind & getter);
    void addPipe(Processors pipe);
    void addTotalsHavingTransform(ProcessorPtr transform);
    void addExtremesTransform(ProcessorPtr transform);
    void addCreatingSetsTransform(ProcessorPtr transform);
    void setOutput(ProcessorPtr output);

    /// Add totals which returns one chunk with single row with defaults.
    void addDefaultTotals();

    /// Add already calculated totals.
    void addTotals(ProcessorPtr source);

    void dropTotalsIfHas();

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);

    /// Check if resize transform was used. (In that case another distinct transform will be added).
    bool hasMixedStreams() const { return has_resize || hasMoreThanOneStream(); }

    void resize(size_t num_streams, bool force = false, bool strict = false);

    void enableQuotaForCurrentStreams();

    void unitePipelines(std::vector<QueryPipeline> && pipelines, const Block & common_header, const Context & context);

    PipelineExecutorPtr execute();

    size_t getNumStreams() const { return streams.size(); }

    bool hasMoreThanOneStream() const { return getNumStreams() > 1; }
    bool hasTotals() const { return totals_having_port != nullptr; }

    const Block & getHeader() const { return current_header; }

    void addTableLock(const TableStructureReadLockHolder & lock) { table_locks.push_back(lock); }
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

    /// Convert query pipeline to single pipe.
    Pipe getPipe() &&;

private:
    /// Destruction order: processors, header, locks, temporary storages, local contexts

    /// Some Streams (or Processors) may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
    TableStructureReadLocks table_locks;

    /// Common header for each stream.
    Block current_header;

    /// All added processors.
    Processors processors;

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
    static void checkSource(const ProcessorPtr & source, bool can_have_totals);

    template <typename TProcessorGetter>
    void addSimpleTransformImpl(const TProcessorGetter & getter);

    void initRowsBeforeLimit();
};

}
