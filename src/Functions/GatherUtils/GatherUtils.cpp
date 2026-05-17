#include <Functions/GatherUtils/GatherUtils.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

}

namespace DB::GatherUtils
{

void sliceHas(IArraySource & first, IArraySource & second, ArraySearchType search_type, ColumnUInt8 & result)
{
    switch (search_type)
    {
        case ArraySearchType::All:
            sliceHasAll(first, second, result);
            break;
        case ArraySearchType::Any:
            sliceHasAny(first, second, result);
            break;
        case ArraySearchType::Substr:
            sliceHasSubstr(first, second, result);
            break;
        case ArraySearchType::StartsWith:
            sliceHasStartsWith(first, second, result);
            break;
        case ArraySearchType::EndsWith:
            sliceHasEndsWith(first, second, result);
            break;
    }
}

void checkQueryCancellation()
{
    /// Route through `QueryStatus::throwIfKilled` whenever the thread is attached
    /// to a query that owns a `ProcessListElement`. This preserves the existing
    /// error-code semantics from the rest of query execution: a query killed by
    /// `max_execution_time` surfaces as `TIMEOUT_EXCEEDED`, a user/error cancel
    /// surfaces as `QUERY_WAS_CANCELLED` (or rethrows a stored cancellation
    /// exception when one was attached to the `QueryStatus`).
    if (auto query_context = CurrentThread::tryGetQueryContext())
    {
        if (auto query_status = query_context->getProcessListElementSafe())
        {
            query_status->throwIfKilled();
            return;
        }
    }

    /// Fallback for code paths that have no query context attached to the process
    /// list (background tasks that wire a cancellation predicate directly into
    /// `ThreadStatus`). We cannot tell timeout from user-cancel here, so we keep
    /// the previous behaviour and report `QUERY_WAS_CANCELLED`.
    if (CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}

}
