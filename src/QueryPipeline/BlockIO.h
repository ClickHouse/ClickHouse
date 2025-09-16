#pragma once

#include <exception>
#include <functional>
#include <Common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

class ProcessListEntry;

struct BlockIO
{
    BlockIO() = default;
    BlockIO(BlockIO &&) = default;

    BlockIO & operator= (BlockIO && rhs) noexcept;
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    std::shared_ptr<ProcessListEntry> process_list_entry;

    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(QueryPipeline &&, std::chrono::system_clock::time_point)> finish_callback;
    std::function<void(bool)> exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    /// Needed to optionally detach from the thread group on destruction
    std::unique_ptr<CurrentThread::QueryScope> query_scope_holder;

    void onFinish(std::chrono::system_clock::time_point finish_time = std::chrono::system_clock::now());
    void onException(bool log_as_error=true);
    void onCancelOrConnectionLoss();

    template<typename Func>
    void executeWithCallbacks(Func && func)
    try
    {
        func();
        onFinish();
    }
    catch (...)
    {
        onException();
        throw;
    }

    /// Set is_all_data_sent in system.processes for this query.
    void setAllDataSent() const;

private:
    void reset();
};

}
