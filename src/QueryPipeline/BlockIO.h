#pragma once

#include <exception>
#include <functional>
#include <vector>
#include <Common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <QueryPipeline/QueryPipeline.h>
#include <IO/Progress.h>


namespace DB
{

class ProcessListEntry;

/// Information prepared by BlockIO::finalize_query_pipeline before calling finish callbacks.
struct QueryFinishInfo
{
    std::chrono::system_clock::time_point finish_time;
    std::optional<ResultProgress> result_progress;
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

    QueryPipeline pipeline;

    /// The finalize_query_pipeline function is called once to flush the pipeline progress and reset it.
    /// Then all finish callbacks are called with the resulting QueryFinishInfo.
    std::function<QueryFinishInfo(QueryPipeline &&)> finalize_query_pipeline;
    std::vector<std::function<void(const QueryFinishInfo &)>> finish_callbacks;

    std::vector<std::function<void(bool)>> exception_callbacks;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    /// Needed to optionally detach from the thread group on destruction
    std::unique_ptr<CurrentThread::QueryScope> query_scope_holder;

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

private:
    void reset();
};

}
