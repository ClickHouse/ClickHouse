#pragma once
#include <Processors/IProcessor.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

class TableStructureReadLock;
using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;

class Context;

class QueryPipeline
{
public:
    QueryPipeline() = default;

    /// Each source must have single output port and no inputs. All outputs must have same header.
    void init(Processors sources);
    bool initialized() { return !processors.empty(); }

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header)>;

    void addSimpleTransform(ProcessorGetter getter);
    void addPipe(Processors pipe);
    void addTotalsHavingTransform(ProcessorPtr transform);
    void addExtremesTransform(ProcessorPtr transform);
    void addCreatingSetsTransform(ProcessorPtr transform);
    void setOutput(ProcessorPtr output);

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);
    bool hasDelayedStream() const { return has_delayed_stream; }

    void resize(size_t num_streams);

    void unitePipelines(std::vector<QueryPipeline> && pipelines, const Context & context);

    void execute(size_t num_threads);

    size_t getNumStreams() const { return streams.size(); }
    size_t getNumMainStreams() const { return streams.size() - (has_delayed_stream ? 1 : 0); }

    const Block & getHeader() const { return current_header; }

    void addTableLock(const TableStructureReadLockPtr & lock) { table_locks.push_back(lock); }

    /// For compatibility with IBlockInputStream.
    void setProcessListEntry(std::shared_ptr<ProcessListEntry> entry) { process_list_entry = std::move(entry); }
    void setProgressCallback(const ProgressCallback & callback);
    void setProcessListElement(QueryStatus * elem);

    std::function<void(IBlockInputStream *, IBlockOutputStream *)>    finish_callback;
    std::function<void()>                                             exception_callback;

private:

    /// All added processors.
    Processors processors;

    /// Port for each independent "stream".
    std::vector<OutputPort *> streams;

    /// Special ports for extremes and totals having.
    OutputPort * totals_having_port = nullptr;
    OutputPort * extremes_port = nullptr;

    /// Common header for each stream.
    Block current_header;

    TableStructureReadLocks table_locks;

    bool has_delayed_stream = false;
    bool has_totals_having = false;
    bool has_extremes = false;
    bool has_output = false;

    /** process_list_entry should be destroyed after in and after out,
      *  since in and out contain pointer to objects inside process_list_entry (query-level MemoryTracker for example),
      *  which could be used before destroying of in and out.
      */
    std::shared_ptr<ProcessListEntry> process_list_entry;

    void checkInitialized();
    void checkSource(const ProcessorPtr & source);
    void concatDelayedStream();
};

}
