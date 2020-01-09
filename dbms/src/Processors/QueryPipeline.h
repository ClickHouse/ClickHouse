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
public:
    QueryPipeline() = default;

    /// All pipes must have same header.
    void init(Pipes pipes);
    void init(Pipe pipe); /// Simple init for single pipe
    bool initialized() { return !processors.empty(); }

    enum class StreamType
    {
        Main = 0,
        Totals,
        Extremes,
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
    bool hasDelayedStream() const { return delayed_stream_port; }
    /// Check if resize transform was used. (In that case another distinct transform will be added).
    bool hasMixedStreams() const { return has_resize || hasMoreThanOneStream(); }

    void resize(size_t num_streams, bool force = false);

    void enableQuotaForCurrentStreams();

    void unitePipelines(std::vector<QueryPipeline> && pipelines, const Block & common_header, const Context & context);

    PipelineExecutorPtr execute();

    size_t getNumStreams() const { return streams.size() + (hasDelayedStream() ? 1 : 0); }
    size_t getNumMainStreams() const { return streams.size(); }

    bool hasMoreThanOneStream() const { return getNumStreams() > 1; }
    bool hasTotals() const { return totals_having_port != nullptr; }

    const Block & getHeader() const { return current_header; }

    void addTableLock(const TableStructureReadLockHolder & lock) { table_locks.push_back(lock); }
    void addInterpreterContext(std::shared_ptr<Context> context) { interpreter_context.emplace_back(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { storage_holder.emplace_back(std::move(storage)); }

    /// For compatibility with IBlockInputStream.
    void setProgressCallback(const ProgressCallback & callback);
    void setProcessListElement(QueryStatus * elem);

    /// Call after execution.
    void finalize();

    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }
    size_t getMaxThreads() const { return max_threads; }

private:

    /// All added processors.
    Processors processors;

    /// Port for each independent "stream".
    std::vector<OutputPort *> streams;

    /// Special ports for extremes and totals having.
    OutputPort * totals_having_port = nullptr;
    OutputPort * extremes_port = nullptr;

    /// Special port for delayed stream.
    OutputPort * delayed_stream_port = nullptr;

    /// If resize processor was added to pipeline.
    bool has_resize = false;

    /// Common header for each stream.
    Block current_header;

    TableStructureReadLocks table_locks;

    /// Some Streams (or Processors) may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holder;

    IOutputFormat * output_format = nullptr;

    size_t max_threads = 0;

    void checkInitialized();
    void checkSource(const ProcessorPtr & source, bool can_have_totals);
    void concatDelayedStream();

    template <typename TProcessorGetter>
    void addSimpleTransformImpl(const TProcessorGetter & getter);

    void calcRowsBeforeLimit();
};

}
