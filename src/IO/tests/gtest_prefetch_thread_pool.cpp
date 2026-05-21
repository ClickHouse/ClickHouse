#include <IO/PrefetchThreadPool.h>
#include <IO/Rope.h>

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>

using namespace DB;

namespace
{

/// Latch the worker thread on entry to a task so callers can manipulate
/// other tasks' queue/run state deterministically.
struct Latch
{
    std::mutex m;
    std::condition_variable cv;
    bool released = false;

    void wait()
    {
        std::unique_lock lk(m);
        cv.wait(lk, [&]{ return released; });
    }

    void release()
    {
        {
            std::lock_guard lk(m);
            released = true;
        }
        cv.notify_all();
    }
};

}

TEST(PrefetchThreadPool, CancelWhenQueued)
{
    /// Pool with a single worker. Block the worker with a latch on task #1,
    /// queue task #2 behind it, then cancel #2 before the worker can pick
    /// it up. Verify #2's body never ran.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    Latch worker_latch;
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
    /// cancelled task. The cancelled task should no-op.
    worker_latch.release();
    std::ignore = blocker->get();

    /// Wait a bit for the worker to process the cancelled task.
    for (int i = 0; i < 100 && handle->state() == PrefetchHandle::State::Cancelled; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    EXPECT_FALSE(task2_body_ran.load()) << "Cancelled task body must not run";
}

TEST(PrefetchThreadPool, WaitWhenRunning)
{
    /// Submit a task that signals start, then waits for release. After the
    /// signal, tryCancel must return false (task is Running) and get() must
    /// return the produced value.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::atomic<bool> started{false};
    Latch release_latch;

    auto handle = pool.submit([&]() -> Rope
    {
        started.store(true);
        release_latch.wait();
        Rope r;
        auto buf = std::make_shared<OwnedRopeBuffer>(4);
        std::memcpy(buf->data(), "Done", 4);
        r.append(RopeNode{buf, 0, 4, 0});
        return r;
    });
    ASSERT_NE(handle, nullptr);

    /// Wait until the worker enters the task.
    while (!started.load())
        std::this_thread::sleep_for(std::chrono::microseconds(100));

    EXPECT_EQ(handle->state(), PrefetchHandle::State::Running);
    EXPECT_FALSE(handle->tryCancel());

    release_latch.release();

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

    /// Wait for completion.
    for (int i = 0; i < 1000 && handle->state() != PrefetchHandle::State::Done; ++i)
        std::this_thread::sleep_for(std::chrono::microseconds(100));

    ASSERT_EQ(handle->state(), PrefetchHandle::State::Done);
    EXPECT_FALSE(handle->tryCancel());
    EXPECT_NO_THROW(std::ignore = handle->get());
}

TEST(PrefetchThreadPool, QueueOverflowReturnsNullptr)
{
    /// pool_size=1, queue_size=3 → at most 3 scheduled jobs (running + queued)
    /// per ThreadPool semantics. Submitting more than 3 with the worker
    /// blocked must return nullptr without blocking.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/3);

    Latch worker_latch;
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
    worker_latch.release();
    std::ignore = blocker->get();
    std::ignore = q1->get();
    std::ignore = q2->get();
}

TEST(PrefetchThreadPool, CancelledFutureGetRethrowsKnownException)
{
    /// If a caller incorrectly waits on a cancelled handle's future, they
    /// must get a definite exception (not a broken_promise hang).

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    Latch worker_latch;
    auto blocker = pool.submit([&]() -> Rope { worker_latch.wait(); return {}; });
    ASSERT_NE(blocker, nullptr);

    auto handle = pool.submit([]() -> Rope { return {}; });
    ASSERT_NE(handle, nullptr);
    ASSERT_TRUE(handle->tryCancel());

    /// Let the worker process the cancelled task — it will set the
    /// "task was cancelled" exception on the promise.
    worker_latch.release();
    std::ignore = blocker->get();

    EXPECT_THROW(std::ignore = handle->get(), std::runtime_error);
}
