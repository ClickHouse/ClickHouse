#include <Common/QueryScope.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
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

QueryScope::QueryScope(bool initialized_)
: initialized(initialized_)
{}

QueryScope::QueryScope(QueryScope && other) noexcept
: initialized(other.initialized)
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

    initialized = other.initialized;
    log_peak_memory_usage_in_destructor = other.log_peak_memory_usage_in_destructor;
    other.initialized = false;
    return *this;
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
