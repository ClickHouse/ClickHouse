#pragma once

#include <functional>
#include <Common/CurrentThread.h>
#include <Interpreters/QueryMetadataCache.h>
#include <QueryPipeline/QueryPipeline.h>
#include <IO/Progress.h>
#include <Processors/IProcessor.h>


namespace DB
{

class ProcessListEntry;

struct QueryPipelineFinalizedInfo
{
    std::optional<ResultProgress> result_progress;
    std::vector<IProcessor::ProcessorsProfileLogInfo> processors_profile_infos;
    String pipeline_dump;
};

struct BlockIO
{
    BlockIO() = default;
    BlockIO(BlockIO &&) = default;

    BlockIO & operator= (BlockIO && rhs) noexcept;
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    /// Needed for internal queries.
    /// Each level calls executeQuery and adds its process list entry.
    std::vector<std::shared_ptr<ProcessListEntry>> process_list_entries;

    /// Query-scoped cache for storage metadata and snapshots.
    ///
    /// The cache is created at query execution entry point and is kept alive by BlockIO for the entire lifetime of
    /// query execution (including pipeline execution and internal/nested queries).
    ///
    /// It allows consistently reusing StorageMetadata and StorageSnapshot instances within the same query across
    /// concurrent execution threads, while avoiding extending their lifetime beyond the query scope.
    ///
    /// The cache is *not* owned by Context to prevent reference cycles; Context only holds a weak reference to it for
    /// access during query execution.
    QueryMetadataCachePtr query_metadata_cache;

    QueryPipeline pipeline;

    /// The finalize_query_pipeline function is called once to flush the pipeline progress and reset it.
    /// Then all finish callbacks are called with the resulting QueryPipelineFinalizedInfo.
    std::function<QueryPipelineFinalizedInfo(QueryPipeline &&)> finalize_query_pipeline;
    std::vector<std::function<void(const QueryPipelineFinalizedInfo &, std::chrono::system_clock::time_point)>> finish_callbacks;

    std::vector<std::function<void(bool)>> exception_callbacks;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    /// Needed to optionally detach from the thread group on destruction
    CurrentThread::QueryScope query_scope;

    void onFinish(std::chrono::system_clock::time_point finish_time = std::chrono::system_clock::now());
    void onException(bool log_as_error=true);
    void onCancelOrConnectionLoss();

    template<typename Func>
    void executeWithCallbacks(Func && func)
    {
        try
        {
            func();
        }
        catch (...)
        {
            onException();
            throw;
        }

        onFinish();
    }

    /// Set is_all_data_sent in system.processes for this query.
    void setAllDataSent() const;

    /// Release query slot early to allow client to reuse it for his next query.
    void releaseQuerySlot() const;

    void resetPipeline(bool cancel);

private:
    void reset();
};

}
