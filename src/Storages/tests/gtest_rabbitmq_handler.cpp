#include <gtest/gtest.h>
#include <config.h>

#if USE_AMQPCPP

#include <Storages/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/logger_useful.h>

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

#endif
