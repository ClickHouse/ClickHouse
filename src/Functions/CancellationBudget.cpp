#include <Functions/CancellationBudget.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

/// Resolved from the executing thread rather than from the context that built the caller: a function
/// instance can be stored in table metadata and then run by any later query.
QueryStatusPtr tryGetProcessListElement()
{
    if (auto query_context = CurrentThread::tryGetQueryContext())
        return query_context->getProcessListElementSafe();
    return nullptr;
}

}

std::function<void()> makeCancellationCheck(const char * function_name)
{
    QueryStatusPtr process_list_element = tryGetProcessListElement();
    if (!process_list_element)
        return {};

    /// A false return means the deadline passed under the `break` overflow mode; a partial result from a
    /// function is a wrong value rather than a smaller one, so it throws either way.
    return [process_list_element, function_name]
    {
        if (ThreadStatus::QueryCancellationBlocker::isActive())
            return;
        if (!process_list_element->checkTimeLimit())
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded: elapsed time limit reached in function {}", function_name);
    };
}

void rethrowIfQueryCancelled(const Exception & e)
{
    /// `throwIf` can raise this code too, so it is a timeout only if the query is past its deadline.
    if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
    {
        QueryStatusPtr process_list_element = tryGetProcessListElement();
        if (process_list_element && !process_list_element->checkTimeLimitSoft())
            throw;
    }
    CurrentThread::checkIfNotCancelled();
}

}
