#pragma once

#include <Interpreters/Context_fwd.h>

#include <functional>
#include <memory>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

/// Initializes query with current thread as master thread in constructor, and detaches it in destructor.
/// Formerly nested as CurrentThread::QueryScope.
class QueryScope
{
private:
    explicit QueryScope(bool initialized_);
public:
    QueryScope() = default;
    QueryScope(QueryScope & other) = delete;
    QueryScope(QueryScope && other) noexcept;
    QueryScope & operator=(QueryScope & other) = delete;
    QueryScope & operator=(QueryScope && other) noexcept;

    static QueryScope create(ContextPtr query_context, std::function<void()> fatal_error_callback = {});
    static QueryScope create(ContextMutablePtr query_context, std::function<void()> fatal_error_callback = {});
    static QueryScope createForFlushAsyncInsert(ContextMutablePtr query_context, ThreadGroupPtr parent);

    ~QueryScope();

    bool initialized = false;
    void logPeakMemoryUsage();
    bool log_peak_memory_usage_in_destructor = true;
};

}
