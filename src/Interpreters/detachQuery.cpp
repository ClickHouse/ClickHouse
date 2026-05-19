#include <Interpreters/detachQuery.h>

#include <Common/CurrentThread.h>
#include <Common/PODArray.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include <future>
#include <memory>
#include <utility>


namespace DB
{

DetachedQueryHandle detachQuery(String query_text, ContextMutablePtr context)
{
    /// Drop any progress callback bound to the caller's connection — the background thread
    /// must not call it after the handler has returned.
    context->setProgressCallback(nullptr);

    auto started_promise = std::make_shared<std::promise<void>>();
    auto started_future = started_promise->get_future();
    auto completion_promise = std::make_shared<std::promise<void>>();
    auto completion_future = std::make_shared<std::future<void>>(completion_promise->get_future());

    GlobalThreadPool::instance().scheduleOrThrow(
        [context, query_for_thread = std::move(query_text), started_promise, completion_promise]() mutable
        {
            setThreadName(ThreadName::QUERY_ASYNC_EXECUTOR);
            ThreadStatus thread_status;
            QueryScope async_query_scope = QueryScope::create(context);
            bool query_started = false;

            /// Invoked by `executeQuery` after `ProcessList::insert` and quota/permission
            /// checks pass — the earliest point where `ExceptionBeforeStart` can no longer
            /// occur.
            auto on_started = [&]()
            {
                if (!query_started)
                {
                    query_started = true;
                    started_promise->set_value();
                }
            };

            try
            {
                PODArray<char> discard_buf;
                WriteBufferFromVector<PODArray<char>> discard_ostr(discard_buf);
                executeQuery(
                    std::make_unique<ReadBufferFromString>(query_for_thread),
                    discard_ostr,
                    context,
                    SetResultDetailsFunc{},
                    QueryFlags{},
                    std::nullopt,
                    HandleExceptionInOutputFormatFunc{},
                    QueryFinishCallback{},
                    HTTPContinueCallback{},
                    std::move(on_started));
                completion_promise->set_value();
            }
            catch (...)
            {
                if (!query_started)
                {
                    /// Pre-start failure: signal it via `started_promise` (caller is blocked on
                    /// it and will rethrow). Do not also store on `completion_promise` — the
                    /// caller has already observed the exception synchronously.
                    started_promise->set_exception(std::current_exception());
                    completion_promise->set_value();
                }
                else
                {
                    /// Post-start failure: the caller has already returned `query_id` to the
                    /// client. Store on `completion` for callers that wait on it (e.g.
                    /// `clickhouse-local` rethrows on the next query), and log on the
                    /// background thread for callers that discard it (HTTP/native server).
                    tryLogCurrentException(getLogger("detachQuery"), "Detached query failed after start");
                    completion_promise->set_exception(std::current_exception());
                }
            }
        });

    /// Block until the background thread signals start (or rethrows a pre-start exception).
    started_future.get();

    return DetachedQueryHandle{context->getClientInfo().current_query_id, std::move(completion_future)};
}

}
