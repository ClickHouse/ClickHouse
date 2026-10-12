#include <Functions/CancellationBudget.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

std::function<void()> makeCancellationCheck(const char * function_name)
{
    /// Resolved from the executing thread rather than from the context that built the caller: a function
    /// instance can be stored in table metadata and then run by any later query.
    QueryStatusPtr process_list_element;
    if (auto query_context = CurrentThread::tryGetQueryContext())
        process_list_element = query_context->getProcessListElementSafe();

    if (!process_list_element)
        return {};

    /// A false return means the deadline passed under the `break` overflow mode; a partial result from a
    /// function is a wrong value rather than a smaller one, so it throws either way.
    return [process_list_element, function_name]
    {
        if (!process_list_element->checkTimeLimit())
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded: elapsed time limit reached in function {}", function_name);
    };
}

}
