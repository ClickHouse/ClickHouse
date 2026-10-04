#include <gtest/gtest.h>
#include <config.h>

#if USE_AMQPCPP

#include <Storages/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <thread>

using namespace DB;

/// Regression test for the dead-connection hang (issue #108496). When RabbitMQ closes the
/// connection without a clean AMQP handshake, the broker callbacks that would call
/// stopBlockingLoop() never fire. startBlockingLoopWithTimeout() must still return, reporting
/// a timeout, instead of blocking forever on uv_run().
TEST(RabbitMQHandler, BlockingLoopReturnsOnTimeoutWithoutStopper)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));

    /// Nothing ever calls stopBlockingLoop(), mimicking a dead broker connection.
    /// The call must come back via the timeout rather than hang (if it hangs, the test times out).
    bool finished_naturally = handler.startBlockingLoopWithTimeout(/* timeout_ms = */ 100);

    EXPECT_FALSE(finished_naturally);
}

/// The timeout must not fire spuriously: when something does call stopBlockingLoop() (as the AMQP
/// success/error callbacks do on a healthy connection), the helper returns true and does not wait
/// for the full timeout. The stopper runs as an on-loop timer, exactly like an AMQP callback that
/// is dispatched from within uv_run() on the loop thread.
TEST(RabbitMQHandler, BlockingLoopReturnsTrueWhenStopped)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));

    uv_timer_t stopper;
    uv_timer_init(loop.getLoop(), &stopper);
    stopper.data = &handler;
    uv_timer_start(
        &stopper,
        [](uv_timer_t * t) { static_cast<RabbitMQHandler *>(t->data)->stopBlockingLoop(); },
        /* timeout_ms = */ 50,
        /* repeat = */ 0);

    bool finished_naturally = handler.startBlockingLoopWithTimeout(/* timeout_ms = */ 30000);

    uv_close(reinterpret_cast<uv_handle_t *>(&stopper), nullptr);
    uv_run(loop.getLoop(), UV_RUN_NOWAIT);

    EXPECT_TRUE(finished_naturally);
}

/// The timeout must span the requested interval no matter how long the loop sat idle beforehand.
/// libuv derives a timer's deadline from a clock the loop caches while it runs, so an idle loop
/// holds an arbitrarily old value, and a timer armed against it is already overdue and fires on the
/// first iteration. A wait that returns immediately is indistinguishable from a broker that never
/// answered, which turns every bounded wait into a no-op.
TEST(RabbitMQHandler, BlockingLoopHonoursTimeoutAfterIdlePeriod)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));

    /// One turn so the loop caches a timestamp, then let that cached value age well past the
    /// timeout used below. Nothing may drive the loop during the wait, as that would refresh it.
    uv_run(loop.getLoop(), UV_RUN_NOWAIT);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    /// Nothing calls stopBlockingLoop(), so the only way out is the timeout itself.
    auto started_at = std::chrono::steady_clock::now();
    bool finished_naturally = handler.startBlockingLoopWithTimeout(/* timeout_ms = */ 500);
    auto elapsed = std::chrono::steady_clock::now() - started_at;

    EXPECT_FALSE(finished_naturally);
    /// Only a lower bound is asserted: a loaded machine can overshoot the timeout, but it cannot
    /// make the wait end early.
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 400);
}

#endif
