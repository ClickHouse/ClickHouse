#include <iostream>
#include <stdexcept>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>


static bool check()
{
    ThreadPool pool(10);

    /// The throwing thread.
    pool.scheduleOrThrowOnError([] { throw std::runtime_error("Hello, world!"); });

    try
    {
        while (true)
        {
            /// An exception from the throwing thread will be rethrown from this method
            /// as soon as the throwing thread executed.

            /// This innocent thread may or may not be executed, the following possibilities exist:
            /// 1. The throwing thread has already thrown exception and the attempt to schedule the innocent thread will rethrow it.
            /// 2. The throwing thread has not executed, the innocent thread will be scheduled and executed.
            /// 3. The throwing thread has not executed, the innocent thread will be scheduled but before it will be executed,
            ///    the throwing thread will be executed and throw exception and it will prevent starting of execution of the innocent thread
            ///    the method will return and the exception will be rethrown only on call to "wait" or on next call on next loop iteration as (1).
            pool.scheduleOrThrowOnError([]{});

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    catch (const std::runtime_error &)
    {
        pool.wait();
        return true;
    }

    __builtin_unreachable();
}


TEST(ThreadPool, ExceptionFromSchedule)
{
    EXPECT_TRUE(check());
}
