#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <functional>
#include <memory>

namespace DB
{

class ThreadGroup;
struct MemoryTrackerSwitcher;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

/// Initializes query with current thread as master thread in constructor, and detaches it in destructor.
/// Formerly nested as CurrentThread::QueryScope.
class QueryScope
{
private:
    explicit QueryScope(bool initialized_);
    ThreadGroupPtr setup_group;
    std::unique_ptr<MemoryTrackerSwitcher> setup_memory_scope;
public:
    QueryScope();
    QueryScope(QueryScope & other) = delete;
    QueryScope(QueryScope && other) noexcept;
    QueryScope & operator=(QueryScope & other) = delete;
    QueryScope & operator=(QueryScope && other) noexcept;

    static QueryScope create(ContextPtr query_context, std::function<void()> fatal_error_callback = {});
    static QueryScope create(ContextMutablePtr query_context, std::function<void()> fatal_error_callback = {});
    /// Declare this scope before the query context, so failed setup frees it before restoring the tracker.
    /// Setup uses unbatched accounting; attachment applies the final query's batching settings.
    static QueryScope createForQueryContext();
    /// Recovery users can retain their configured batching allowance while receiving a query.
    static QueryScope createForQueryContext(Int64 untracked_memory_limit);
    void attachToQueryContext(ContextMutablePtr query_context, std::function<void()> fatal_error_callback = {});
    static QueryScope createForFlushAsyncInsert(ContextMutablePtr query_context, ThreadGroupPtr parent);

    ~QueryScope();

    bool initialized = false;
    void logPeakMemoryUsage();
    bool log_peak_memory_usage_in_destructor = true;
};

}
