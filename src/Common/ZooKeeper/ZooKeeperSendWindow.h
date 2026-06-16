#pragma once

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <base/scope_guard.h>

namespace Coordination
{

/// Runs the send-thread "request window" under a callback guard.
///
/// The window is the span between popping a request from the queue and registering it in
/// `operations` (from which receiveEvent()/finalize() later satisfy its callback). During that span
/// the local RequestInfo is the SOLE owner of the callback. Async callers such as
/// zkutil::ZooKeeper::asyncTryExistsNoThrow capture a std::shared_ptr<std::promise<...>> in that
/// callback and hand the std::future to a waiting caller before the request is processed. If
/// `window_body` throws (the OpenTelemetry span finalize, addRootPath, or the `operations` map insert
/// can all throw, e.g. on allocation under memory pressure), the RequestInfo is destroyed while
/// unwinding and the callback never runs. ~promise() then abandons an unsatisfied shared state, so the
/// waiter's future.get() observes a broken-promise std::future_error -- a std::logic_error that aborts
/// the server.
///
/// This function arms a guard BEFORE running `window_body`: if the body throws, the guard satisfies the
/// callback with an error response (ZCONNECTIONLOSS if the request was probably sent, otherwise
/// ZSESSIONEXPIRED) instead of letting the promise be abandoned. On success the body has registered the
/// request in `operations` (which now owns callback satisfaction), so the guard is disarmed and does
/// nothing. Production sendThread() and the regression test both call this same function, so the guard
/// cannot be absent or ordered after the throwing body without the test observing it.
template <typename RequestInfo, typename WindowBody>
void runGuardedSendWindow(RequestInfo & info, LoggerPtr log, WindowBody && window_body)
{
    /// Set only after `window_body` succeeds; until then the guard is responsible for the callback.
    bool callback_registered = false;
    SCOPE_EXIT({
        if (callback_registered || !info.callback)
            return;
        try
        {
            ZooKeeperResponsePtr response = info.request->makeResponse();
            response->error = info.request->probably_sent ? Error::ZCONNECTIONLOSS : Error::ZSESSIONEXPIRED;
            response->xid = info.request->xid;
            info.callback(*response);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// A scope-exit guard must never let an exception escape during unwinding. Log if we have a
            /// logger; in test contexts (log == nullptr) there is nothing else to do.
            if (log)
                DB::tryLogCurrentException(log);
        }
    });

    window_body();
    callback_registered = true;
}

}
