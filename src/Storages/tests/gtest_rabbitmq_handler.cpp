#include <gtest/gtest.h>
#include <config.h>

#if USE_AMQPCPP

#include <Storages/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <future>
#include <latch>
#include <stdexcept>
#include <thread>

using namespace DB;

/// run() executes on the loop thread while it runs, inline otherwise, and does not deadlock when called on it.
TEST(RabbitMQHandler, RunExecutesOnLoopThread)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));
    const auto self = std::this_thread::get_id();
    auto current_thread = [] { return std::this_thread::get_id(); };

    EXPECT_FALSE(handler.loopRunning());
    EXPECT_EQ(handler.run(current_thread), self);

    handler.startLoop();
    EXPECT_TRUE(handler.loopRunning());
    const auto loop_thread = handler.run(current_thread);
    EXPECT_NE(loop_thread, self);
    EXPECT_EQ(handler.run([&] { return handler.run(current_thread); }), loop_thread);
    EXPECT_THROW(handler.run([] { throw std::runtime_error("boom"); }), std::runtime_error);

    handler.stopLoop();
    EXPECT_FALSE(handler.loopRunning());
    EXPECT_EQ(handler.run(current_thread), self);
}

/// An exception leaking from a callback ends the loop thread; run() then executes inline and startLoop() replaces it.
TEST(RabbitMQHandler, CallbackExceptionDoesNotWedgeRun)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));
    const auto self = std::this_thread::get_id();
    auto current_thread = [] { return std::this_thread::get_id(); };

    handler.startLoop();
    uv_timer_t thrower;
    handler.run([&]
    {
        uv_timer_init(loop.getLoop(), &thrower);
        uv_timer_start(&thrower, [](uv_timer_t *) { throw std::runtime_error("boom"); }, /* timeout_ms = */ 1, /* repeat = */ 0);
    });

    for (size_t i = 0; i < 500 && handler.loopRunning(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(handler.loopRunning());

    /// Dead thread: no hang, inline execution.
    EXPECT_EQ(handler.run(current_thread), self);

    handler.startLoop();
    EXPECT_TRUE(handler.loopRunning());
    EXPECT_NE(handler.run(current_thread), self);
    handler.run([&] { uv_close(reinterpret_cast<uv_handle_t *>(&thrower), nullptr); });
    handler.stopLoop();
}

/// Counts tasks and callbacks that run at the same time. AMQP-CPP is not thread safe, so there must be none.
struct OverlapCheck
{
    std::atomic<int> inside{0};
    std::atomic<bool> overlap{false};
    std::atomic<size_t> calls{0};

    void enter()
    {
        if (inside.fetch_add(1) != 0)
            overlap = true;
        ++calls;
    }
    void leave() { inside.fetch_sub(1); }
};

/// start / post / stop against a thread that calls run() continuously: no two tasks may overlap.
TEST(RabbitMQHandler, StartPostStopRace)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));

    OverlapCheck check;
    std::atomic<size_t> executed_by_poster{0};
    auto task = [&]
    {
        check.enter();
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        check.leave();
    };

    std::atomic<bool> done{false};
    std::thread poster([&]
    {
        while (!done)
        {
            handler.run(task);
            ++executed_by_poster;
            /// The mutexes are not fair: without a pause this thread would starve startLoop() and stopLoop().
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    });

    for (size_t i = 0; i < 300 && !check.overlap; ++i)
    {
        handler.startLoop();
        handler.run(task);
        handler.stopLoop();
        handler.run(task);
    }

    done = true;
    poster.join();
    EXPECT_FALSE(check.overlap) << "two tasks ran at the same time";
    EXPECT_GE(check.calls, 600u);
    EXPECT_GT(executed_by_poster, 0u) << "the competing thread never ran a task";
}

/// startLoop() must wait for an inline call (table setup, cleanup) to finish before the loop thread may
/// dispatch libuv callbacks. A repeating timer stands in for socket events: it must not fire during the call.
TEST(RabbitMQHandler, InlineCallBlocksLoopStart)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));
    OverlapCheck check;

    uv_timer_t timer;
    uv_timer_init(loop.getLoop(), &timer);
    timer.data = &check;
    auto on_timer = [](uv_timer_t * t)
    {
        auto * c = static_cast<OverlapCheck *>(t->data);
        c->enter();
        c->leave();
    };
    uv_timer_start(&timer, on_timer, /* timeout_ms = */ 1, /* repeat = */ 1);

    std::latch inline_call_started(1);
    std::thread caller([&]
    {
        handler.run([&]
        {
            check.enter();
            inline_call_started.count_down();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            check.leave();
        });
    });
    inline_call_started.wait();
    handler.startLoop();
    caller.join();

    const size_t calls_before = check.calls;
    for (size_t i = 0; i < 500 && check.calls < calls_before + 5; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_GE(check.calls, calls_before + 5) << "the timer never fired on the loop thread";

    handler.run([&] { uv_close(reinterpret_cast<uv_handle_t *>(&timer), nullptr); });
    handler.stopLoop();
    EXPECT_FALSE(check.overlap) << "a libuv callback ran during an inline call";
}

/// The loop thread blocks between events instead of spinning.
TEST(RabbitMQHandler, IdleLoopThreadDoesNotSpin)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));
    handler.startLoop();

    auto loop_thread_cpu_ms = [&]
    {
        return handler.run([]
        {
            timespec ts{};
            clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
            return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
        });
    };

    const auto before = loop_thread_cpu_ms();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const auto after = loop_thread_cpu_ms();
    handler.stopLoop();

    /// A spinning loop would burn ~500 ms here.
    EXPECT_LT(after - before, 50);
}

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

/// A stopLoop() wake-up still pending after the thread exits must not stop the next blocking wait.
TEST(RabbitMQHandler, BlockingLoopAfterStopLoopHonoursTimeout)
{
    UVLoop loop;
    RabbitMQHandler handler(loop.getLoop(), getLogger("RabbitMQHandlerTest"));
    handler.startLoop();

    std::promise<void> entered;
    std::atomic<bool> release{false};
    std::thread busy([&]
    {
        handler.run([&]
        {
            entered.set_value();
            while (!release)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    });
    entered.get_future().wait();
    std::thread stopper([&] { handler.stopLoop(); });
    /// Let stopLoop() send its wake-up while the task still occupies the loop thread.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    release = true;
    busy.join();
    stopper.join();

    auto started_at = std::chrono::steady_clock::now();
    bool finished_naturally = handler.startBlockingLoopWithTimeout(/* timeout_ms = */ 300);
    auto elapsed = std::chrono::steady_clock::now() - started_at;

    EXPECT_FALSE(finished_naturally);
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 250);
}

#endif
