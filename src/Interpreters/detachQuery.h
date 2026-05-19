#pragma once

#include <Interpreters/Context_fwd.h>

#include <base/types.h>

#include <future>
#include <memory>


namespace DB
{

/// Handle returned by `detachQuery`. The `query_id` is safe to send to the client immediately
/// (the background thread has already passed `ProcessList::insert` / quota / permission checks by
/// the time `detachQuery` returns). `completion` resolves when the background task finishes —
/// either with no value (success) or with a stored exception (post-start failure). Long-lived
/// servers (HTTP, native TCP) discard the handle; `clickhouse-local` keeps it so it can wait for
/// the work to finish before the process exits and surface any post-start exception on the next
/// query.
struct DetachedQueryHandle
{
    String query_id;
    std::shared_ptr<std::future<void>> completion;
};


/// Schedule a query on `GlobalThreadPool` and block only until it has passed all pre-execution
/// checks (ProcessList::insert, quotas, permissions). Returns once the query is guaranteed to no
/// longer raise `ExceptionBeforeStart`, so the caller can safely report the `query_id` to the
/// client. Output is discarded; progress callbacks are suppressed.
///
/// Re-throws any pre-start exception so the caller can propagate it via its normal error path
/// (HTTP error page, native exception packet, etc.). Post-start exceptions are observable on the
/// returned `completion` future (and also logged on the background thread).
///
/// `query_text` must be the full query text the worker should execute. For HTTP, callers pass
/// the URL query concatenated with the POST body so that `INSERT ... FORMAT ...` is detachable;
/// for native and clickhouse-local, callers pass the SQL text only (those protocols deliver
/// `INSERT` data on a separate packet, so such queries must run synchronously and are filtered
/// out by the caller).
///
/// The caller is responsible for eligibility, typically:
///   * `allow_experimental_detach_queries` is on and `async_insert` is off
///   * `IAST::isDetachableQuery(ast)` is true
///   * for native / clickhouse-local: the query is not `INSERT ... FORMAT ...` that needs
///     client data on a separate packet.
DetachedQueryHandle detachQuery(String query_text, ContextMutablePtr context);

}
