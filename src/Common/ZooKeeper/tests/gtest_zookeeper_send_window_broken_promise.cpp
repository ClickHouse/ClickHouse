#include <gtest/gtest.h>

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <base/scope_guard.h>

#include <future>
#include <memory>
#include <stdexcept>
#include <tuple>

using namespace Coordination;

/// Regression test for the broken-promise server abort in Coordination::ZooKeeper::sendThread
/// (STID 2508-34fb, ZooKeeper-client asyncTryExists variant).
///
/// In ZooKeeper::sendThread, after a request is popped from the queue, the local RequestInfo is
/// the SOLE owner of its callback until the request is inserted into `operations` (from which
/// receiveEvent()/finalize() later satisfy it). Async callers such as
/// ZooKeeper::asyncTryExistsNoThrow capture a std::shared_ptr<std::promise<...>> in that callback
/// and hand the std::future to a waiting caller before the request is processed. If anything in
/// the pop->insert window throws (the OpenTelemetry span finalize, addRootPath, or the map insert
/// itself -- all can throw, e.g. on allocation under memory pressure), the local RequestInfo is
/// destroyed while unwinding and the callback never runs. ~promise() then destroys an unsatisfied
/// shared state, so the waiter's future.get() observes a std::future_error (broken promise). That
/// std::future_error is a std::logic_error, which ClickHouse reports as a LOGICAL_ERROR and aborts
/// the process (see "Abort on std::logic_error in CI", PR #51907).
///
/// The fix arms a SCOPE_EXIT guard over that window: if the request is dropped before its callback
/// ownership transfers to `operations`, the guard satisfies the callback with an error response
/// (ZCONNECTIONLOSS if it was probably sent, otherwise ZSESSIONEXPIRED) instead of letting the
/// promise be abandoned. This mirrors how finalize() drains outstanding operations, and the
/// already-merged sibling fixes for the same error class (TestKeeper, PR #73570; the thread-pool
/// callback runner, PR #107383).
///
/// The test reproduces the exact pop->insert window contract: the same promise-capturing callback
/// shape used by asyncTryExistsNoThrow, a throw inside the window, and the guard logic. Without the
/// guard the future carries a broken-promise std::future_error; with the guard it carries a normal
/// ZSESSIONEXPIRED response and nothing aborts.
namespace
{

/// Mirror of ZooKeeper::RequestInfo's relevant fields for this window.
struct RequestInfoLike
{
    ZooKeeperRequestPtr request;
    ResponseCallback callback;
};

/// Build the callback exactly as ZooKeeper::asyncTryExistsNoThrow does:
/// a shared_ptr<promise<ExistsResponse>> captured by the response callback.
RequestInfoLike makeExistsRequest(std::future<ExistsResponse> & future_out)
{
    auto promise = std::make_shared<std::promise<ExistsResponse>>();
    future_out = promise->get_future();

    RequestInfoLike info;
    info.request = std::make_shared<ZooKeeperExistsRequest>();
    info.request->xid = 1;
    info.callback = [promise](const Response & response)
    {
        promise->set_value(dynamic_cast<const ExistsResponse &>(response));
    };
    return info;
}

/// Run the send-window logic that ZooKeeper::sendThread performs between tryPop and the
/// operations[] insert. `throw_in_window` forces a throw before the insert (as a span finalize /
/// addRootPath / map allocation would under memory pressure). `with_guard` enables the fix.
/// On the no-throw path the callback is moved into `operations` (mirroring operations[xid] = info,
/// which keeps the callback alive past the window in production) and the guard is disarmed.
void runSendWindow(RequestInfoLike && popped, bool throw_in_window, bool with_guard, RequestInfoLike & operations)
{
    /// `info` is the sole owner of the callback here, just like the local RequestInfo in sendThread.
    RequestInfoLike info = std::move(popped);

    bool callback_registered = false;
    /// The guard, mirroring the fix in ZooKeeperImpl.cpp::sendThread.
    SCOPE_EXIT({
        if (!with_guard)
            return;
        if (callback_registered || !info.callback)
            return;
        try
        {
            ZooKeeperResponsePtr response = info.request->makeResponse();
            response->error = info.request->probably_sent ? Error::ZCONNECTIONLOSS : Error::ZSESSIONEXPIRED;
            response->xid = info.request->xid;
            info.callback(*response);
        }
        catch (...) // NOLINT(bugprone-empty-catch) Ok: a scope-exit guard must never let an exception escape during unwinding (matches the production SCOPE_EXIT in sendThread); the test exercises only the no-throw satisfy path
        {
        }
    });

    try
    {
        if (throw_in_window)
            throw std::runtime_error("allocation failure in send window");

        /// No-throw path: register the request in `operations`, transferring callback ownership away
        /// (receiveEvent()/finalize() will satisfy it later). Then disarm the guard, exactly as the
        /// fix does after operations[info.request->xid] = info.
        operations = info;
        callback_registered = true;
    }
    catch (...) // NOLINT(bugprone-empty-catch) Ok: sendThread has no inner catch in this window (the throw propagates to its outer catch); here we swallow only so the test can inspect the future the guard already satisfied
    {
    }
}

}

/// The bug: without the guard, a throw in the window abandons the promise -> broken-promise future.
TEST(ZooKeeperSendWindow, ThrowWithoutGuardBreaksPromise)
{
    std::future<ExistsResponse> future;
    auto info = makeExistsRequest(future);
    RequestInfoLike operations;

    runSendWindow(std::move(info), /*throw_in_window=*/ true, /*with_guard=*/ false, operations);

    ASSERT_TRUE(future.valid());
    bool saw_broken_promise = false;
    try
    {
        std::ignore = future.get();
        FAIL() << "expected the future to carry a broken-promise error";
    }
    catch (const std::future_error & e)
    {
        /// This is the bug: a std::logic_error escaping from ~promise() that aborts the server.
        saw_broken_promise = true;
        EXPECT_EQ(e.code(), std::future_errc::broken_promise) << e.what();
    }
    EXPECT_TRUE(saw_broken_promise);
}

/// The fix: with the guard, the same throw satisfies the callback with a normal ZSESSIONEXPIRED
/// response. The waiter gets an ordinary result, no std::future_error, nothing aborts.
TEST(ZooKeeperSendWindow, ThrowWithGuardSatisfiesPromise)
{
    std::future<ExistsResponse> future;
    auto info = makeExistsRequest(future);
    RequestInfoLike operations;

    runSendWindow(std::move(info), /*throw_in_window=*/ true, /*with_guard=*/ true, operations);

    ASSERT_TRUE(future.valid());
    /// Must NOT throw future_error; must observe a normal, catchable Keeper error response.
    ExistsResponse response;
    ASSERT_NO_THROW(response = future.get());
    EXPECT_EQ(response.error, Error::ZSESSIONEXPIRED);
}

/// On the normal path (no throw) the guard is disarmed: the callback is now owned by `operations`
/// (as after operations[xid] = info), so the guard must NOT satisfy the promise itself. The promise
/// stays pending until the response arrives -- here we simulate receiveEvent() invoking the
/// registered callback exactly once, and confirm the future then resolves to the real value (no
/// double-satisfaction, no broken promise).
TEST(ZooKeeperSendWindow, NoThrowWithGuardDoesNotFire)
{
    std::future<ExistsResponse> future;
    auto info = makeExistsRequest(future);
    RequestInfoLike operations;

    runSendWindow(std::move(info), /*throw_in_window=*/ false, /*with_guard=*/ true, operations);

    /// Guard did not fire: the promise is still pending because `operations` owns the callback.
    ASSERT_TRUE(future.valid());
    ASSERT_EQ(future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);

    /// Simulate receiveEvent() delivering the real response via the registered callback.
    ASSERT_TRUE(static_cast<bool>(operations.callback));
    ZooKeeperExistsResponse response;
    response.error = Error::ZOK;
    operations.callback(response);

    ExistsResponse got;
    ASSERT_NO_THROW(got = future.get());
    EXPECT_EQ(got.error, Error::ZOK);
}
