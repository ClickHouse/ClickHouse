#pragma once

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Processors/Pipe.h>
#include <atomic>

namespace DB
{

class RemoteQueryExecutor;
using RemoteQueryExecutorPtr = std::shared_ptr<RemoteQueryExecutor>;

class RemoteQueryExecutorReadContext;

/// Source from RemoteQueryExecutor. Executes remote query and returns query result chunks.
class RemoteSource : public SourceWithProgress
{
public:
    /// Flag add_aggregation_info tells if AggregatedChunkInfo should be added to result chunk.
    /// AggregatedChunkInfo stores the bucket number used for two-level aggregation.
    /// This flag should be typically enabled for queries with GROUP BY which are executed till WithMergeableState.
    RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_);
    ~RemoteSource() override;

    Status prepare() override;
    String getName() const override { return "Remote"; }

    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) { rows_before_limit.swap(counter); }

    /// Stop reading from stream if output port is finished.
    void onUpdatePorts() override;

    int schedule() override { return fd; }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    std::atomic<bool> was_query_canceled = false;
    bool was_query_sent = false;
    bool add_aggregation_info = false;
    RemoteQueryExecutorPtr query_executor;
    RowsBeforeLimitCounterPtr rows_before_limit;

    const bool async_read;
    bool is_async_state = false;
    std::unique_ptr<RemoteQueryExecutorReadContext> read_context;
    int fd = -1;
};

/// Totals source from RemoteQueryExecutor.
class RemoteTotalsSource : public ISource
{
public:
    explicit RemoteTotalsSource(RemoteQueryExecutorPtr executor);
    ~RemoteTotalsSource() override;

    String getName() const override { return "RemoteTotals"; }

protected:
    Chunk generate() override;

private:
    RemoteQueryExecutorPtr query_executor;
};

/// Extremes source from RemoteQueryExecutor.
class RemoteExtremesSource : public ISource
{
public:
    explicit RemoteExtremesSource(RemoteQueryExecutorPtr executor);
    ~RemoteExtremesSource() override;

    String getName() const override { return "RemoteExtremes"; }

protected:
    Chunk generate() override;

private:
    RemoteQueryExecutorPtr query_executor;
};

/// Create pipe with remote sources.
Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read);

}
