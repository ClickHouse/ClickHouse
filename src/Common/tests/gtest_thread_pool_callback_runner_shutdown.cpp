#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <future>
#include <vector>
#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

using namespace DB;

/// Regression test for the broken-promise abort (STID 2508-38c6).
///
/// A task scheduled through threadPoolCallbackRunnerUnsafe that the pool drops unrun during
/// shutdown must NOT leave its std::promise unsatisfied. A std::packaged_task destroyed unrun
/// stores a broken-promise std::future_error (a std::logic_error) into the shared state; when a
/// waiter observes it via future.get(), the server reports it as a LOGICAL_ERROR
/// (getCurrentExceptionMessageAndPattern -> abortOnFailedAssertion), aborting the process.
///
/// The runner must instead store a normal DB::Exception so future.get() throws an ordinary,
/// catchable error and nothing aborts. DB::Exception is NOT a std::logic_error, so even if the
/// waiter feeds it through the server's exception reporting it will not abort.
TEST(ThreadPoolCallbackRunner, DroppedOnShutdownDoesNotBreakPromise)
{
    for (size_t iteration = 0; iteration < 20; ++iteration)
    {
        constexpr size_t num_victims = 32;

        std::vector<std::future<void>> victim_futures;
        victim_futures.reserve(num_victims);

        std::promise<void> release;
        std::shared_future<void> release_future = release.get_future().share();

        {
            /// shutdown_on_exception = true (default): when the blocker throws, the worker sets
            /// shutdown=true and then drains the remaining queued jobs without running them.
            ThreadPool pool(
                CurrentMetrics::LocalThread,
                CurrentMetrics::LocalThreadActive,
                CurrentMetrics::LocalThreadScheduled,
                /*max_threads=*/ 1,
                /*max_free_threads=*/ 1,
                /*queue_size=*/ 1000);

            /// Occupy the single worker. It blocks until we release it, guaranteeing the victim
            /// tasks below are all queued behind it. On release it throws, which triggers shutdown.
            pool.scheduleOrThrowOnError([release_future]()
            {
                release_future.wait();
                throw std::runtime_error("trigger shutdown_on_exception");
            });

            auto runner = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::UNKNOWN);
            for (size_t i = 0; i < num_victims; ++i)
                victim_futures.push_back(runner([]() {}, Priority{}));

            /// Release the blocker -> it throws -> worker sets shutdown and drains the victims unrun.
            release.set_value();

            try
            {
                pool.wait();
            }
            catch (const std::exception & e)
            {
                /// The blocker's std::runtime_error is rethrown here; expected. Catch the concrete
                /// std::exception (not catch-all) so anything unexpected propagates and fails the test.
                SUCCEED() << "pool.wait() rethrew the expected shutdown exception: " << e.what();
            }
            /// Pool destructor (finalize) runs at scope exit and joins the worker.
        }

        /// Each victim either ran (set_value) or was dropped on shutdown. A dropped task must carry
        /// a normal DB::Exception, never a broken-promise std::future_error. Catch future_error
        /// specifically and fail if we see it -- that is the bug.
        for (auto & future : victim_futures)
        {
            ASSERT_TRUE(future.valid());
            try
            {
                future.get();
            }
            catch (const std::future_error & e)
            {
                FAIL() << "iteration " << iteration
                       << ": victim task was dropped without satisfying its promise (broken promise): "
                       << e.what();
            }
            catch (const Exception & e)
            {
                /// Expected for dropped tasks: a normal, catchable DB::Exception (CANNOT_SCHEDULE_TASK).
                EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK) << e.what();
            }
        }
    }
}
