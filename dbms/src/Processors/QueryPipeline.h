#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class TableStructureReadLock;
using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;

class Context;

class IOutputFormat;

class QueryPipeline
{
public:
    QueryPipeline() = default;

    /// Each source must have single output port and no inputs. All outputs must have same header.
    void init(Processors sources);
    bool initialized() { return !processors.empty(); }

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header)>;

    void addSimpleTransform(const ProcessorGetter & getter);
    void addPipe(Processors pipe);
    void addTotalsHavingTransform(ProcessorPtr transform);
    void addExtremesTransform(ProcessorPtr transform);
    void addCreatingSetsTransform(ProcessorPtr transform);
    void setOutput(ProcessorPtr output);

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);
    bool hasDelayedStream() const { return delayed_stream_port; }

    void resize(size_t num_streams);

    void unitePipelines(std::vector<QueryPipeline> && pipelines, const Context & context);

    PipelineExecutorPtr execute(size_t num_threads);

    size_t getNumStreams() const { return streams.size() + (hasDelayedStream() ? 1 : 0); }
    size_t getNumMainStreams() const { return streams.size(); }
    bool hasMoreThanOneStream() const { return getNumStreams() > 1; }

    const Block & getHeader() const { return current_header; }

    void addTableLock(const TableStructureReadLockPtr & lock) { table_locks.push_back(lock); }

    /// For compatibility with IBlockInputStream.
    void setProgressCallback(const ProgressCallback & callback);
    void setProcessListElement(QueryStatus * elem);

    /// Call after execution.
    void finalize();

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

    /// Common header for each stream.
    Block current_header;

    TableStructureReadLocks table_locks;

    IOutputFormat * output_format = nullptr;

    PipelineExecutorPtr executor;
    std::shared_ptr<ThreadPool> pool;

    void checkInitialized();
    void checkSource(const ProcessorPtr & source);
    void concatDelayedStream();

    void calcRowsBeforeLimit();
};

}
