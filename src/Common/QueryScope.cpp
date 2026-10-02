#include <Common/QueryScope.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void QueryScope::logPeakMemoryUsage()
{
    auto group = CurrentThread::getGroup();
    if (!group)
        return;

    log_peak_memory_usage_in_destructor = false;
    group->memory_tracker.logPeakMemoryUsage();
}

QueryScope::QueryScope() = default;

QueryScope::QueryScope(bool initialized_)
: initialized(initialized_)
{}

QueryScope::QueryScope(QueryScope && other) noexcept
: setup_group(std::move(other.setup_group))
, setup_memory_scope(std::move(other.setup_memory_scope))
, initialized(other.initialized)
, log_peak_memory_usage_in_destructor(other.log_peak_memory_usage_in_destructor)
{
    other.initialized = false;
}

QueryScope & QueryScope::operator=(QueryScope && other) noexcept
{
    if (this == &other)
        return *this;

    if (initialized)
    {
        try
        {
            if (log_peak_memory_usage_in_destructor)
                logPeakMemoryUsage();
            CurrentThread::detachFromGroupIfNotDetached();
        }
        catch (...)
        {
            tryLogCurrentException("QueryScope", __PRETTY_FUNCTION__);
        }
    }

    setup_memory_scope.reset();
    setup_group = std::move(other.setup_group);
    setup_memory_scope = std::move(other.setup_memory_scope);
    initialized = other.initialized;
    log_peak_memory_usage_in_destructor = other.log_peak_memory_usage_in_destructor;
    other.initialized = false;
    return *this;
}

QueryScope QueryScope::createForQueryContext()
{
    return createForQueryContext(0);
}

QueryScope QueryScope::createForQueryContext(Int64 untracked_memory_limit)
{
    if (CurrentThread::getGroup() || CurrentThread::get().memory_tracker.getParent() != &total_memory_tracker)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot start query context setup inside another query accounting scope");

    QueryScope scope;
    scope.setup_group = std::make_shared<ThreadGroup>();
    scope.setup_memory_scope = std::make_unique<MemoryTrackerSwitcher>(&scope.setup_group->memory_tracker, untracked_memory_limit);
    return scope;
}

void QueryScope::attachToQueryContext(ContextMutablePtr query_context, std::function<void()> fatal_error_callback)
{
    if (!setup_group || initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context setup must be started before attachment");

    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();
    {
        /// Group metadata outlives query detachment.
        MemoryTrackerSwitcher group_memory_scope(&total_memory_tracker);
        setup_group->initializeQuery(query_context, fatal_error_callback);
    }
    setup_memory_scope->reset();
    try
    {
        CurrentThread::attachToGroup(setup_group);
    }
    catch (...)
    {
        /// Attachment detaches on failure. Keep query-context cleanup in its original tracker.
        setup_memory_scope->switchTo(&setup_group->memory_tracker, 0);
        throw;
    }
    {
        /// The heap guard was allocated before setup accounting began.
        MemoryTrackerSwitcher guard_memory_scope(&total_memory_tracker);
        setup_memory_scope.reset();
    }
    initialized = true;
    setup_group.reset();
}

QueryScope QueryScope::create(ContextPtr query_context, std::function<void()> fatal_error_callback)
{
    if (!query_context->hasQueryContext())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot initialize query scope without query context");

    auto group = ThreadGroup::createForQuery(query_context, std::move(fatal_error_callback));
    CurrentThread::attachToGroup(group);
    return QueryScope(true);
}

QueryScope QueryScope::create(ContextMutablePtr query_context, std::function<void()> fatal_error_callback)
{
    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();

    auto group = ThreadGroup::createForQuery(query_context, std::move(fatal_error_callback));
    CurrentThread::attachToGroup(group);
    return QueryScope(true);
}

QueryScope QueryScope::createForFlushAsyncInsert(ContextMutablePtr query_context, ThreadGroupPtr parent)
{
    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();

    auto group = ThreadGroup::createForFlushAsyncInsertQueue(query_context, parent);
    CurrentThread::attachToGroup(group);
    return QueryScope(true);
}

QueryScope::~QueryScope()
{
    if (!initialized)
        return;

    try
    {
        if (log_peak_memory_usage_in_destructor)
            logPeakMemoryUsage();

        CurrentThread::detachFromGroupIfNotDetached();
    }
    catch (...)
    {
        tryLogCurrentException("CurrentThread", __PRETTY_FUNCTION__);
    }
}

}
