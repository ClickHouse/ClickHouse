#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <thread>
#include <gtest/gtest.h>
#include <Common/PoolBase.h>
#include <Poco/Logger.h>
using namespace DB;

namespace DB::ErrorCodes
{
    extern const int NO_FREE_CONNECTION;
}

class PoolObject
{
public:
    int x = 0;
};

class MyPoolBase : public PoolBase<PoolObject>
{
public:
    using Object = PoolBase<PoolObject>::Object;
    using ObjectPtr = std::shared_ptr<Object>;
    using Ptr = PoolBase<PoolObject>::Ptr;

    int last_destroy_value = 0;
    MyPoolBase() : PoolBase<PoolObject>(100, getLogger("MyPoolBase")) { }

protected:
    ObjectPtr allocObject() override { return std::make_shared<Object>(); }

    void expireObject(ObjectPtr obj) override
    {
        LOG_TRACE(log, "expire object");
        ASSERT_TRUE(obj->x == 100);
        last_destroy_value = obj->x;
    }
};

TEST(PoolBase, testDestroy1)
{
    MyPoolBase pool;
    {
        auto obj_entry = pool.get(-1);
        ASSERT_TRUE(!obj_entry.isNull());
        obj_entry->x = 100;
        obj_entry.expire();
    }
    ASSERT_EQ(1, pool.size());

    {
        auto obj_entry = pool.get(-1);
        ASSERT_TRUE(!obj_entry.isNull());
        ASSERT_EQ(obj_entry->x, 0);
        ASSERT_EQ(1, pool.size());
    }
    ASSERT_EQ(100, pool.last_destroy_value);
}

/// Holds a single object, so the next get() has to wait.
class OneObjectPool : public PoolBase<PoolObject>
{
public:
    OneObjectPool() : PoolBase<PoolObject>(1, getLogger("OneObjectPool")) { }

protected:
    ObjectPtr allocObject() override { return std::make_shared<Object>(); }
};

/// Result of calling get() on a saturated pool from another thread.
struct WaiterOutcome
{
    bool finished = false;
    Int64 elapsed_ms = -1;
    int code = 0;       /// error code thrown, 0 if get() returned an object
};

/// Runs pool.get(timeout) on its own thread and reports whether it finished within `budget_ms`.
/// A thread still blocked at that point is detached, so a waiter that ignores its timeout fails the
/// assertion instead of hanging the whole test binary.
template <typename Pool>
static WaiterOutcome waitForGet(Pool & pool, Poco::Timespan::TimeDiff timeout, Int64 budget_ms)
{
    /// Shared so that a detached thread outliving this call still writes to live storage. `done`
    /// publishes it: the release store below is paired with the acquire load in the poll loop.
    auto outcome = std::make_shared<WaiterOutcome>();
    auto done = std::make_shared<std::atomic<bool>>(false);

    std::thread thread([&pool, timeout, outcome, done]
    {
        Stopwatch watch;
        int code = 0;
        try
        {
            auto entry = pool.get(timeout);
        }
        catch (const DB::Exception & e)
        {
            code = e.code();
        }
        outcome->code = code;
        outcome->elapsed_ms = watch.elapsedMilliseconds();
        outcome->finished = true;
        done->store(true, std::memory_order_release);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_ms);
    while (!done->load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    if (!done->load(std::memory_order_acquire))
    {
        thread.detach();
        return {};
    }
    thread.join();
    return *outcome;
}

/// A positive timeout has to expire. Without a deadline in the wait loop this blocks for good.
TEST(PoolBase, finiteTimeoutExpires)
{
    OneObjectPool pool;
    auto held = pool.get(-1);

    const auto outcome = waitForGet(pool, /* timeout= */ 300, /* budget_ms= */ 5000);

    ASSERT_TRUE(outcome.finished) << "get(300) did not return within 5000 ms";
    ASSERT_EQ(outcome.code, DB::ErrorCodes::NO_FREE_CONNECTION);
    /// Upper bound only: the deadline is what the wait honours, and a loaded machine can add to it.
    ASSERT_GE(outcome.elapsed_ms, 250);
    ASSERT_LT(outcome.elapsed_ms, 3000) << "waited " << outcome.elapsed_ms << " ms for a 300 ms timeout";
}

/// A negative timeout still means "wait until an object is free", so this must not gain a deadline.
TEST(PoolBase, infiniteTimeoutKeepsWaiting)
{
    OneObjectPool pool;
    auto held = std::make_optional(pool.get(-1));

    /// Long enough to outlast several slices, so slicing alone cannot end the wait. The waiter is
    /// polled through the same catching helper as the other arms, so an implementation that ends
    /// this wait by throwing is reported as a failed assertion rather than an uncaught exception.
    const auto premature = waitForGet(pool, /* timeout= */ -1, /* budget_ms= */ 2500);
    ASSERT_FALSE(premature.finished)
        << "get(-1) stopped waiting on its own after " << premature.elapsed_ms << " ms, code " << premature.code;

    /// Releasing has to hand the object to that same waiter, so the wait ends on notification.
    held.reset();
    const auto outcome = waitForGet(pool, /* timeout= */ -1, /* budget_ms= */ 20000);
    ASSERT_TRUE(outcome.finished) << "get(-1) did not return after the object was freed";
    ASSERT_EQ(outcome.code, 0);
}

/// The wait ends as soon as an object is returned, well before the deadline.
TEST(PoolBase, finiteTimeoutReturnsFreedObject)
{
    OneObjectPool pool;
    auto held = std::make_optional(pool.get(-1));

    std::thread releaser([&]
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        held.reset();
    });

    const auto outcome = waitForGet(pool, /* timeout= */ 30000, /* budget_ms= */ 20000);
    releaser.join();

    ASSERT_TRUE(outcome.finished) << "get(30000) did not return after the object was freed";
    ASSERT_EQ(outcome.code, 0) << "threw code " << outcome.code << " although an object became free";
    ASSERT_LT(outcome.elapsed_ms, 10000) << "waited " << outcome.elapsed_ms << " ms for an object freed after 200 ms";
}

/// A timeout spanning more than one slice must still expire at the deadline, not at the end of the
/// slice that crosses it. 1010 ms is deliberately just over one slice: the last slice has 10 ms left
/// to run, so a slice that is not clamped to the remaining time waits a further full second and the
/// call takes about 2000 ms instead of about 1010.
TEST(PoolBase, lastSliceIsClampedToTheDeadline)
{
    OneObjectPool pool;
    auto held = pool.get(-1);

    const auto outcome = waitForGet(pool, /* timeout= */ 1010, /* budget_ms= */ 30000);

    ASSERT_TRUE(outcome.finished) << "get(1010) did not return within 30000 ms";
    ASSERT_EQ(outcome.code, DB::ErrorCodes::NO_FREE_CONNECTION);
    ASSERT_GE(outcome.elapsed_ms, 950);
    ASSERT_LT(outcome.elapsed_ms, 1500) << "waited " << outcome.elapsed_ms << " ms for a 1010 ms timeout";
}

/// A timeout larger than a steady_clock::time_point can hold must still mean "wait", not "expired":
/// computing the deadline without clamping wraps it into the past and the wait fails immediately.
TEST(PoolBase, hugeTimeoutDoesNotWrap)
{
    OneObjectPool pool;
    auto held = std::make_optional(pool.get(-1));

    std::thread releaser([&]
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        held.reset();
    });

    const auto outcome = waitForGet(
        pool, /* timeout= */ std::numeric_limits<Poco::Timespan::TimeDiff>::max() / 1000, /* budget_ms= */ 20000);
    releaser.join();

    ASSERT_TRUE(outcome.finished);
    ASSERT_EQ(outcome.code, 0) << "an effectively unbounded timeout expired with code " << outcome.code;
}

