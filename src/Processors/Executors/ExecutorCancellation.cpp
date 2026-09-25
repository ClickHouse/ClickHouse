#include <Processors/Executors/ExecutorCancellation.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/Exception.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

ExecutorCancellation::ExecutorCancellation(std::function<bool()> callback_, Policy policy_, ContextPtr context_)
    : callback(std::move(callback_))
    , policy(policy_)
    , context(std::move(context_))
{
}

ExecutorCancellation ExecutorCancellation::cancelExecution(std::function<bool()> callback_)
{
    return {std::move(callback_), Policy::CancelExecution, {}};
}

ExecutorCancellation ExecutorCancellation::finishPartialResult(std::function<bool()> callback_)
{
    return {std::move(callback_), Policy::FinishPartialResult, {}};
}

ExecutorCancellation ExecutorCancellation::cancelQuery(std::function<bool()> callback_, ContextPtr context_)
{
    return {std::move(callback_), Policy::CancelQuery, std::move(context_)};
}

void ExecutorCancellation::check(PipelineExecutor & executor) const
{
    if (!callback || !callback())
        return;

    switch (policy)
    {
        case Policy::CancelExecution:
            executor.cancel();
            return;
        case Policy::FinishPartialResult:
            executor.cancelReading();
            return;
        case Policy::CancelQuery:
        {
            if (auto process_list_element = context->getProcessListElementSafe())
            {
                process_list_element->throwIfKilled();

                auto exception = std::make_exception_ptr(
                    Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "Received 'Cancel' packet from the client, canceling the query."));
                process_list_element->cancelQuery(CancelReason::CANCELLED_BY_USER, exception);
                process_list_element->throwIfKilled();

                std::rethrow_exception(exception);
            }

            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "Received 'Cancel' packet from the client, canceling the query.");
        }
    }
}

}
