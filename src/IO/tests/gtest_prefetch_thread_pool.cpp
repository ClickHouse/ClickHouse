#include <IO/PrefetchThreadPool.h>

#include <gtest/gtest.h>
#include <atomic>
#include <latch>
#include <memory>
#include <stdexcept>

using namespace DB;

TEST(PrefetchThreadPool, JobRunsAndGetReturns)
{
    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::atomic<bool> ran{false};
    auto handle = pool.submitJob([&] { ran.store(true); });
    ASSERT_NE(handle, nullptr);

    /// NB: isFinished() is a PRE-get probe — get() consumes the future
    /// (the contract `drainAbandonedMachines` relies on).
    EXPECT_NO_THROW(handle->get());
    EXPECT_TRUE(ran.load());
    EXPECT_EQ(handle->state(), JobHandle::State::Done);
}

TEST(PrefetchThreadPool, JobGetPropagatesException)
{
    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    auto handle = pool.submitJob([] { throw std::runtime_error("job failed"); });
    ASSERT_NE(handle, nullptr);

    EXPECT_THROW(handle->get(), std::runtime_error);
    /// Done, not a poisoned state: the worker stored Done before set_exception.
    EXPECT_EQ(handle->state(), JobHandle::State::Done);
}

TEST(PrefetchThreadPool, JobCancelWhenQueued)
{
    /// Block the single worker, queue a job behind it, cancel it before the
    /// worker can pick it up. The cancelled body must never run.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::latch worker_latch{1};
    auto blocker = pool.submitJob([&] { worker_latch.wait(); });
    ASSERT_NE(blocker, nullptr);

    std::atomic<bool> body_ran{false};
    auto handle = pool.submitJob([&] { body_ran.store(true); });
    ASSERT_NE(handle, nullptr);

    EXPECT_EQ(handle->state(), JobHandle::State::Queued);
    EXPECT_FALSE(handle->isFinished());  /// queued behind the blocked worker — promise unset
    EXPECT_TRUE(handle->tryCancel());
    EXPECT_EQ(handle->state(), JobHandle::State::Cancelled);

    worker_latch.count_down();
    blocker->get();

    /// get() blocks until the worker reached the cancelled task and rethrows
    /// its "task was cancelled" exception — the deterministic sync point.
    EXPECT_THROW(handle->get(), std::runtime_error);
    EXPECT_FALSE(body_ran.load()) << "Cancelled job body must not run";
}

TEST(PrefetchThreadPool, JobWaitWhenRunning)
{
    /// Once the job signalled start, tryCancel must fail (Running) and get()
    /// must join the completed job.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::latch start_latch{1};
    std::latch release_latch{1};

    auto handle = pool.submitJob([&]
    {
        start_latch.count_down();
        release_latch.wait();
    });
    ASSERT_NE(handle, nullptr);

    /// The worker's CAS to Running is sequenced before the task body, so once
    /// the task has entered, observing Running is guaranteed.
    start_latch.wait();
    EXPECT_EQ(handle->state(), JobHandle::State::Running);
    EXPECT_FALSE(handle->tryCancel());

    release_latch.count_down();
    EXPECT_NO_THROW(handle->get());
    EXPECT_EQ(handle->state(), JobHandle::State::Done);
}

TEST(PrefetchThreadPool, JobQueueOverflowReturnsNullptr)
{
    /// pool_size=1, queue_size=3 → at most 3 scheduled jobs (running + queued)
    /// per ThreadPool semantics. Submitting more than 3 with the worker
    /// blocked must return nullptr without blocking.

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/3);

    std::latch worker_latch{1};
    auto blocker = pool.submitJob([&] { worker_latch.wait(); });
    ASSERT_NE(blocker, nullptr);

    auto q1 = pool.submitJob([] {});
    auto q2 = pool.submitJob([] {});
    ASSERT_NE(q1, nullptr);
    ASSERT_NE(q2, nullptr);

    auto overflow = pool.submitJob([] {});
    EXPECT_EQ(overflow, nullptr) << "Submission past queue capacity must return nullptr";

    worker_latch.count_down();
    blocker->get();
    q1->get();
    q2->get();
}

TEST(PrefetchThreadPool, CancelledFutureGetRethrowsKnownException)
{
    /// If a caller incorrectly waits on a cancelled handle's future, they
    /// must get a definite exception (not a broken_promise hang).

    PrefetchThreadPool pool(/*pool_size=*/1, /*queue_size=*/4);

    std::latch worker_latch{1};
    auto blocker = pool.submitJob([&] { worker_latch.wait(); });
    ASSERT_NE(blocker, nullptr);

    auto handle = pool.submitJob([] {});
    ASSERT_NE(handle, nullptr);
    ASSERT_TRUE(handle->tryCancel());

    /// Let the worker process the cancelled task — it will set the
    /// "task was cancelled" exception on the promise.
    worker_latch.count_down();
    blocker->get();

    EXPECT_THROW(handle->get(), std::runtime_error);
}

TEST(PrefetchThreadPool, CompletedJobHandleForTest)
{
    auto handle = PrefetchThreadPool::makeCompletedJobHandleForTest();
    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(handle->isFinished());
    EXPECT_EQ(handle->state(), JobHandle::State::Done);
    EXPECT_FALSE(handle->tryCancel());
    EXPECT_NO_THROW(handle->get());
}
