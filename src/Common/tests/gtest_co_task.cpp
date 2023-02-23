#include <gtest/gtest.h>
#include <Common/CoTask.h>
#include <base/sleep.h>
#include <boost/algorithm/string.hpp>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int TASK_CANCELLED;
}

using namespace DB;

namespace
{
    class ThreadSafeOutput
    {
    public:
        void append(char c)
        {
            std::lock_guard lock{mutex};
            str += c;
        }

        void append(const std::string & chars)
        {
            std::lock_guard lock{mutex};
            str += chars;
        }

        std::string get()
        {
            std::lock_guard lock{mutex};
            return std::exchange(str, {});
        }

    private:
        std::string TSA_GUARDED_BY(mutex) str;
        mutable std::mutex mutex;
    };

    constexpr size_t SLEEP_TIME = 50;

    /// A simple coroutine. Normally the first argument's type should be rather `std::shared_ptr<ThreadSafeOutput>`, but I wanted to make the test simple.
    Co::Task<> appendChars(ThreadSafeOutput * output, char first_char, size_t count, bool throw_exception = false)
    {
        for (size_t i = 0; i != count; ++i)
        {
            if (throw_exception && (i == count / 2))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Test error");
            char c = static_cast<char>(i + first_char);
            output->append(c);
            sleepForMilliseconds(SLEEP_TIME);
        }
        co_return;
    }

    /// Calls a few `append_chars()` CoTask either sequentially or in parallel.
    Co::Task<> appendCharsMulti(ThreadSafeOutput * output, bool parallel, bool throw_exception = false)
    {
        output->append(parallel ? "par_" : "seq_");

        std::vector<Co::Task<>> tasks;
        tasks.push_back(appendChars(output, 'a', 3));
        tasks.push_back(appendChars(output, 'A', 10, throw_exception));
        tasks.push_back(appendChars(output, 'a', 7));

        Co::Task<> main_task;
        if (parallel)
            main_task = Co::parallel(std::move(tasks));
        else
            main_task = Co::sequential(std::move(tasks));

        output->append("run_");
        co_await std::move(main_task); /// That's how we call another coroutine.

        output->append("_end");
    }

    /// Similar to appendChars(), but this coroutine returns a string.
    Co::Task<std::string> appendCharsWithResult(ThreadSafeOutput * output, char first_char, size_t count, bool throw_exception = false)
    {
        std::string result;
        for (size_t i = 0; i != count; ++i)
        {
            if (throw_exception && (i == count / 2))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Test error");
            char c = static_cast<char>(i + first_char);
            output->append(c);
            result += c;
            sleepForMilliseconds(SLEEP_TIME);
        }
        co_return result;
    }

    /// Similar to appendCharsMulti(), but this coroutine returns a string.
    Co::Task<std::string> appendCharsWithResultMulti(ThreadSafeOutput * output, bool parallel, bool throw_exception = false)
    {
        output->append(parallel ? "par_" : "seq_");
        
        std::vector<Co::Task<std::string>> tasks;
        tasks.push_back(appendCharsWithResult(output, 'a', 3));
        tasks.push_back(appendCharsWithResult(output, 'A', 10, throw_exception));
        tasks.push_back(appendCharsWithResult(output, 'a', 7));

        Co::Task<std::vector<std::string>> main_task;
        if (parallel)
            main_task = Co::parallel(std::move(tasks));
        else
            main_task = Co::sequential(std::move(tasks));

        output->append("run_");
        auto results = co_await std::move(main_task);

        output->append("_end");
        co_return boost::join(results, "|");
    }

    void checkThrowsExpectedException(std::function<void()> && f, const Exception & expected_exception)
    {
        try
        {
            f();
            EXPECT_TRUE(false && "The expected exception wasn't thrown");
        }
        catch(const Exception & e)
        {
            EXPECT_EQ(e.code(), expected_exception.code());
            EXPECT_EQ(e.what(), String{expected_exception.what()});
            if ((e.code() == expected_exception.code()) && (strcmp(e.what(), expected_exception.what()) == 0))
                return;
            tryLogCurrentException("Unexpected exception");
        }
        catch(...)
        {
            EXPECT_TRUE(false);
            tryLogCurrentException("Unexpected exception");
        }
    }

}

/// Actual tests.

TEST(CoTask, Sequential)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;
    appendCharsMulti(&output, /* parallel= */ false).syncRun(Co::Scheduler{thread_pool});
    EXPECT_EQ(output.get(), "seq_run_abcABCDEFGHIJabcdefg_end");
}

TEST(CoTask, Parallel)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;
    appendCharsMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{thread_pool});
    {
        auto output_string = output.get();
        SCOPED_TRACE(output_string);
        EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabBbcCcDdEeFfGgHIJ_end"));
    }
}

TEST(CoTask, ParallelOnSmallerNumThreads)
{
    ThreadPool thread_pool{1, 0, 0};
    ThreadSafeOutput output;
    appendCharsMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{thread_pool});
    {
        auto output_string = output.get();
        SCOPED_TRACE(output_string);
        EXPECT_TRUE(boost::iequals(output_string, "par_run_abcABCDEFGHIJabcdefg_end"));
    }
}

TEST(CoTask, ParallelOnSingleThread)
{
    ThreadSafeOutput output;

    /// A single thread cannot execute tasks in parallel, so we expect the same result as for just sequential tasks.
    {
        appendCharsMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{});
        EXPECT_EQ(output.get(), "par_run_abcABCDEFGHIJabcdefg_end");
    }

    {
        ThreadPool thread_pool{1, 0, 0};
        appendCharsMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{thread_pool});
        EXPECT_EQ(output.get(), "par_run_abcABCDEFGHIJabcdefg_end");
    }
}

TEST(CoTask, WithResult)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    {
        auto result_string = appendCharsWithResultMulti(&output, /* parallel= */ false).syncRun(Co::Scheduler{thread_pool});
        EXPECT_EQ(output.get(), "seq_run_abcABCDEFGHIJabcdefg_end");
        EXPECT_EQ(result_string, "abc|ABCDEFGHIJ|abcdefg");
    }

    {
        auto result_string = appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{thread_pool});
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabBbcCcDdEeFfGgHIJ_end"));
        }
        EXPECT_EQ(result_string, "abc|ABCDEFGHIJ|abcdefg");
    }

    {
        auto result_string = appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{});
        EXPECT_EQ(output.get(), "par_run_abcABCDEFGHIJabcdefg_end");
        EXPECT_EQ(result_string, "abc|ABCDEFGHIJ|abcdefg");
    }
}

TEST(CoTask, ExceptionInTask)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    auto check_throws = [](std::function<void()> && f)
    { checkThrowsExpectedException(std::move(f), Exception(ErrorCodes::BAD_ARGUMENTS, "Test error")); };

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ false, /* throw_exception= */ true).syncRun(Co::Scheduler{thread_pool}); });
        EXPECT_EQ(output.get(), "seq_run_abcABCDE");
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ false, /* throw_exception= */ true).syncRun(Co::Scheduler{thread_pool}); });
        EXPECT_EQ(output.get(), "seq_run_abcABCDE");
    }

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ true, /* throw_exception= */ true).syncRun(Co::Scheduler{thread_pool}); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabbBccCdDeEfg"));
        }
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ true, /* throw_exception= */ true).syncRun(Co::Scheduler{thread_pool}); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabbBccCdDeEfg"));
        }
    }

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ true, /* throw_exception= */ true).syncRun(Co::Scheduler{}); });
        EXPECT_EQ(output.get(), "par_run_abcABCDE");
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ true, /* throw_exception= */ true).syncRun(Co::Scheduler{}); });
        EXPECT_EQ(output.get(), "par_run_abcABCDE");
    }
}

TEST(CoTask, ExceptionInScheduler)
{
    ThreadPool thread_pool{1, 0, 1};
    ThreadSafeOutput output;

    auto check_throws = [](std::function<void()> && f)
    { checkThrowsExpectedException(std::move(f), Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot schedule a task: no free thread (timeout=0) (threads=1, jobs=1)")); };

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{thread_pool}); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_"));
        }
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun(Co::Scheduler{thread_pool}); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_"));
        }
    }
}

TEST(CoTask, CancelTask)
{
    ThreadPool thread_pool{2, 0, 0};
    ThreadSafeOutput output;

    auto check_throws = [](std::function<void()> && f)
    { checkThrowsExpectedException(std::move(f), Exception(ErrorCodes::TASK_CANCELLED, "Task cancelled")); };

    {
        auto awaiter = appendCharsMulti(&output, /* parallel= */ false).run(Co::Scheduler{thread_pool});
        sleepForMilliseconds(SLEEP_TIME / 2);
        awaiter.tryCancelTask();
        check_throws([&] { awaiter.syncWait(); });
        EXPECT_EQ(output.get(), "seq_run_abc");
    }

    {
        auto awaiter = appendCharsMulti(&output, /* parallel= */ true).run(Co::Scheduler{thread_pool});
        sleepForMilliseconds(SLEEP_TIME / 2);
        awaiter.tryCancelTask();
        check_throws([&] { awaiter.syncWait(); });
        auto output_string = output.get();
        SCOPED_TRACE(output_string);
        EXPECT_TRUE(boost::iequals(output_string, "par_run_aAbBcCDEFGHIJ"));
    }
}
