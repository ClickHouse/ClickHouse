#include <Storages/DeferredRefreshErrors.h>

#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <new>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_SLOW;
    extern const int ABORTED;
    extern const int TOO_DEEP_RECURSION;
}

/// A broken invariant, a memory limit, a deadline or a cancellation belongs to the query, not to the table.
static bool isQueryOuterGuardError(int code)
{
    return code == ErrorCodes::LOGICAL_ERROR
        || code == ErrorCodes::MEMORY_LIMIT_EXCEEDED
        || code == ErrorCodes::CANNOT_ALLOCATE_MEMORY
        || code == ErrorCodes::QUERY_WAS_CANCELLED
        || code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT
        || code == ErrorCodes::TIMEOUT_EXCEEDED
        || code == ErrorCodes::TOO_SLOW
        || code == ErrorCodes::ABORTED;
}

void DeferredRefreshErrors::removeFinishedQueries() const
{
    std::erase_if(queries, [](const QueryErrors & errors) { return errors.query.expired(); });
    has_queries = !queries.empty();
}

void DeferredRefreshErrors::refresh(const ContextPtr & context, const StoragePtr & table, const std::function<void()> & refresh_table)
{
    std::shared_ptr<const QueryStatus> query = context->getProcessListElementSafe();
    if (!query)
    {
        refresh_table();
        return;
    }

    /// Above one while the tables being refreshed refresh other tables, e.g. `Merge` tables that read each other.
    static thread_local size_t nesting = 0;
    ++nesting;
    SCOPE_EXIT({ --nesting; });

    try
    {
        refresh_table();
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (...)
    {
        /// Tables that read each other recurse until the stack overflows. Keeping that error at every level would walk
        /// every path through them, so only the outermost refresh keeps it.
        const int code = getCurrentExceptionCode();
        if (isQueryOuterGuardError(code) || (code == ErrorCodes::TOO_DEEP_RECURSION && nesting > 1))
            throw;

        std::lock_guard lock(mutex);
        removeFinishedQueries();
        auto it = std::ranges::find_if(queries, [&](const QueryErrors & errors) { return errors.query.lock() == query; });
        if (it == queries.end())
            it = queries.insert(queries.end(), QueryErrors{query, {}});
        it->errors.push_back({table, std::current_exception()});
        has_queries = true;
        return;
    }

    if (has_queries)
    {
        std::lock_guard lock(mutex);
        removeFinishedQueries();
    }
}

void DeferredRefreshErrors::rethrow(const ContextPtr & context, const StoragePtr & table) const
{
    std::shared_ptr<const QueryStatus> query = context->getProcessListElementSafe();
    if (!query || !has_queries)
        return;

    std::exception_ptr error;
    {
        std::lock_guard lock(mutex);
        removeFinishedQueries();
        for (const auto & errors : queries)
        {
            if (errors.query.lock() != query)
                continue;
            for (const auto & kept : errors.errors)
                if (kept.storage.lock() == table)
                    error = kept.error;
        }
    }

    if (error)
        std::rethrow_exception(error);
}

}
