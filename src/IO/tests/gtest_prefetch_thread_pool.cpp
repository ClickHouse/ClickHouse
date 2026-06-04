#include <IO/PrefetchThreadPool.h>
#include <IO/Rope.h>

#include <gtest/gtest.h>
#include <atomic>
#include <cstring>
#include <latch>
#include <memory>
#include <stdexcept>
#include <tuple>

using namespace DB;

TEST(PrefetchThreadPool, CancelWhenQueued)
{
    /// Pool with a single worker. Block the worker with a latch on task #1,
    /// queue task #2 behind it, then cancel #2 before the worker can pick
    /// it up. Verify #2's body never ran.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::latch worker_latch{1};
    auto blocker = pool.submit([&]() -> Rope
    {
        worker_latch.wait();
        return {};
    });
    ASSERT_NE(blocker, nullptr);

    std::atomic<bool> task2_body_ran{false};
    auto handle = pool.submit([&]() -> Rope
    {
        task2_body_ran.store(true);
        return {};
    });
    ASSERT_NE(handle, nullptr);

    /// Task #2 is queued behind the blocked worker.
    EXPECT_EQ(handle->state(), PrefetchHandle::State::Queued);

    EXPECT_TRUE(handle->tryCancel());
    EXPECT_EQ(handle->state(), PrefetchHandle::State::Cancelled);

    /// Release the blocker so the worker drains the queue and reaches the
    /// cancelled task. The cancelled task is never run; the worker sets a
    /// std::runtime_error on its promise.
    worker_latch.count_down();
    std::ignore = blocker->get();

    /// get() blocks until the worker has reached the cancelled task and rethrows
    /// that exception - a deterministic synchronization point with the cancel
    /// code path, with no sleep / scheduler-timing dependence.
    EXPECT_THROW(std::ignore = handle->get(), std::runtime_error);
    EXPECT_FALSE(task2_body_ran.load()) << "Cancelled task body must not run";
}

TEST(PrefetchThreadPool, WaitWhenRunning)
{
    /// Submit a task that signals start, then waits for release. After the
    /// signal, tryCancel must return false (task is Running) and get() must
    /// return the produced value.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::latch start_latch{1};
    std::latch release_latch{1};

    auto handle = pool.submit([&]() -> Rope
    {
        start_latch.count_down();
        release_latch.wait();
        Rope r;
        auto buf = std::make_shared<OwnedRopeBuffer>(4);
        std::memcpy(buf->data(), "Done", 4);
        r.append(RopeNode{buf, 0, 4, 0});
        return r;
    });
    ASSERT_NE(handle, nullptr);

    /// The worker's CAS to Running is sequenced before the task body, so once
    /// the task has entered, observing Running is guaranteed.
    start_latch.wait();
    EXPECT_EQ(handle->state(), PrefetchHandle::State::Running);
    EXPECT_FALSE(handle->tryCancel());

    release_latch.count_down();

    Rope r = handle->get();
    EXPECT_EQ(r.totalBytes(), 4u);
    EXPECT_EQ(handle->state(), PrefetchHandle::State::Done);
}

TEST(PrefetchThreadPool, TryCancelAfterCompletion)
{
    /// Wait until the task completes, then tryCancel must return false.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    auto handle = pool.submit([]() -> Rope { return {}; });
    ASSERT_NE(handle, nullptr);

    /// get() returns only after the worker published Done (stored before the
    /// future is made ready), so it is the completion sync point.
    EXPECT_NO_THROW(std::ignore = handle->get());
    EXPECT_EQ(handle->state(), PrefetchHandle::State::Done);
    EXPECT_FALSE(handle->tryCancel());
}

TEST(PrefetchThreadPool, QueueOverflowReturnsNullptr)
{
    /// pool_size=1, queue_size=3 → at most 3 scheduled jobs (running + queued)
    /// per ThreadPool semantics. Submitting more than 3 with the worker
    /// blocked must return nullptr without blocking.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/3);

    std::latch worker_latch{1};
    auto blocker = pool.submit([&]() -> Rope { worker_latch.wait(); return {}; });
    ASSERT_NE(blocker, nullptr);

    /// Two more should fit in the queue.
    auto q1 = pool.submit([]() -> Rope { return {}; });
    auto q2 = pool.submit([]() -> Rope { return {}; });
    ASSERT_NE(q1, nullptr);
    ASSERT_NE(q2, nullptr);

    /// Now the pool is at capacity; the next submit must return nullptr.
    auto overflow = pool.submit([]() -> Rope { return {}; });
    EXPECT_EQ(overflow, nullptr) << "Submission past queue capacity must return nullptr";

    /// Cleanup.
    worker_latch.count_down();
    std::ignore = blocker->get();
    std::ignore = q1->get();
    std::ignore = q2->get();
}

TEST(PrefetchThreadPool, CancelledFutureGetRethrowsKnownException)
{
    /// If a caller incorrectly waits on a cancelled handle's future, they
    /// must get a definite exception (not a broken_promise hang).

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::latch worker_latch{1};
    auto blocker = pool.submit([&]() -> Rope { worker_latch.wait(); return {}; });
    ASSERT_NE(blocker, nullptr);

    auto handle = pool.submit([]() -> Rope { return {}; });
    ASSERT_NE(handle, nullptr);
    ASSERT_TRUE(handle->tryCancel());

    /// Let the worker process the cancelled task — it will set the
    /// "task was cancelled" exception on the promise.
    worker_latch.count_down();
    std::ignore = blocker->get();

    EXPECT_THROW(std::ignore = handle->get(), std::runtime_error);
}
