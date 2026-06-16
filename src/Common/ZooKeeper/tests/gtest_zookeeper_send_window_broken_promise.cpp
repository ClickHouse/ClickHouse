#include <gtest/gtest.h>

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperSendWindow.h>

#include <future>
#include <memory>
#include <stdexcept>
#include <tuple>

using namespace Coordination;

/// Regression test for the broken-promise server abort in Coordination::ZooKeeper::sendThread
/// (STID 2508-34fb, ZooKeeper-client asyncTryExists variant).
///
/// In ZooKeeper::sendThread, after a request is popped from the queue, the local RequestInfo is the
/// SOLE owner of its callback until the request is inserted into `operations` (from which
/// receiveEvent()/finalize() later satisfy it). Async callers such as
/// ZooKeeper::asyncTryExistsNoThrow capture a std::shared_ptr<std::promise<...>> in that callback and
/// hand the std::future to a waiting caller before the request is processed. If anything in the
/// pop->insert window throws (the OpenTelemetry span finalize, addRootPath, or the map insert itself --
/// all can throw, e.g. on allocation under memory pressure), the local RequestInfo is destroyed while
/// unwinding and the callback never runs. ~promise() then destroys an unsatisfied shared state, so the
/// waiter's future.get() observes a std::future_error (broken promise). That std::future_error is a
/// std::logic_error, which ClickHouse reports as a LOGICAL_ERROR and aborts the process (see "Abort on
/// std::logic_error in CI", PR #51907).
///
/// The fix wraps that window in Coordination::runGuardedSendWindow, which arms a guard before running
/// the window body and satisfies the callback with an error response (ZCONNECTIONLOSS if probably sent,
/// otherwise ZSESSIONEXPIRED) if the body throws. This test drives the SAME runGuardedSendWindow that
/// production sendThread() calls, with the same promise-capturing callback shape used by
/// asyncTryExistsNoThrow. If the guard is removed from runGuardedSendWindow (or the helper stops arming
/// it before the body runs), ThrowViaGuardedWindowSatisfiesPromise below fails with a broken promise.
namespace
{

/// Mirror of ZooKeeper::RequestInfo's fields that the send window touches. runGuardedSendWindow is a
/// template over the request-info type, so this stand-in exercises the production helper directly.
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

}

/// The bug, reproduced by running the window body WITHOUT the guard (as the code did before the fix):
/// a throw in the window abandons the promise, so the waiter's future carries a broken-promise error.
/// This is the abort that runGuardedSendWindow prevents.
TEST(ZooKeeperSendWindow, ThrowWithoutGuardBreaksPromise)
{
    std::future<ExistsResponse> future;
    {
        auto info = makeExistsRequest(future);
        try
        {
            /// Unguarded window body, mirroring the pre-fix sendThread: a throw before callback
            /// ownership transfers to `operations` destroys `info` (and its captured promise) unrun.
            throw std::runtime_error("allocation failure in send window");
        }
        catch (const std::runtime_error &) // NOLINT(bugprone-empty-catch) Ok: sendThread re-raises to its outer catch; here we only need `info` to have been destroyed by unwinding
        {
        }
    }

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

/// The fix: running the SAME throwing window body through the production runGuardedSendWindow helper
/// makes the guard satisfy the callback with a normal ZSESSIONEXPIRED response. The waiter gets an
/// ordinary result, no std::future_error, nothing aborts. The window-body throw still propagates out
/// (to sendThread's outer catch in production); we catch it here only to inspect the future.
TEST(ZooKeeperSendWindow, ThrowViaGuardedWindowSatisfiesPromise)
{
    std::future<ExistsResponse> future;
    {
        /// `info` is scoped exactly as the local RequestInfo in sendThread: it is destroyed right
        /// after the window (here at the end of this block; in sendThread when the throw leaves the
        /// pop-to-insert scope). With the guard, the callback is satisfied during unwinding before
        /// `info` dies. Without the guard, `info` dies unsatisfied and the promise breaks -- which is
        /// the abort this test guards against, and makes the assertions below fail if the guard is
        /// removed from runGuardedSendWindow.
        auto info = makeExistsRequest(future);

        /// The window-body throw must still propagate (sendThread's outer catch finalizes the session);
        /// the guard satisfies the callback but does not swallow the exception.
        EXPECT_THROW(
            runGuardedSendWindow(info, /*log=*/ nullptr, []
            {
                throw std::runtime_error("allocation failure in send window");
            }),
            std::runtime_error);
    }

    ASSERT_TRUE(future.valid());
    /// Must NOT throw future_error; must observe a normal, catchable Keeper error response.
    ExistsResponse response;
    ASSERT_NO_THROW(response = future.get());
    EXPECT_EQ(response.error, Error::ZSESSIONEXPIRED);
}

/// On the normal path (window body does not throw) the guard is disarmed: the body has registered the
/// request in `operations` (here, a stand-in that keeps the callback alive), so the guard must NOT
/// satisfy the promise itself. The promise stays pending until the response arrives -- we then simulate
/// receiveEvent() invoking the registered callback exactly once and confirm the future resolves to the
/// real value (no double-satisfaction, no broken promise).
TEST(ZooKeeperSendWindow, NoThrowViaGuardedWindowDoesNotFire)
{
    std::future<ExistsResponse> future;
    auto info = makeExistsRequest(future);
    RequestInfoLike operations;

    runGuardedSendWindow(info, /*log=*/ nullptr, [&]
    {
        /// Transfer callback ownership to `operations`, exactly as operations[xid] = info does.
        operations = info;
    });

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
