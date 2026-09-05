#include <optional>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <Common/ThreadFuzzer.h>
#include <Common/Stopwatch.h>

TEST(ThreadFuzzer, mutex)
{
    /// Initialize ThreadFuzzer::started
    DB::ThreadFuzzer::instance().setup();

    std::mutex mutex;
    std::atomic<size_t> elapsed_ns = 0;

    auto func = [&]()
    {
        Stopwatch watch;
        for (size_t i = 0; i < 1'000'000; ++i)
        {
            mutex.lock();
            mutex.unlock();
        }
        elapsed_ns += watch.elapsedNanoseconds();
    };

    std::vector<std::optional<std::thread>> threads(10);

    for (auto & thread : threads)
        thread.emplace(func);

    for (auto & thread : threads)
        thread->join();

    std::cout << "elapsed: " << static_cast<double>(elapsed_ns) / 1e9 << "\n";
}

#if defined(OS_LINUX)

#include <csignal>
#include <cstring>
#include <iostream>
#include <string>

#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <base/errnoToString.h>
#include <Common/Exception.h>

extern char ** environ;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

/// Set by the parent test on the image it exec's. Absent for an in-process run, where the
/// singleton has already read an unconfigured environment and the assertions would be vacuous.
const char * const child_marker_env = "CH_THREAD_FUZZER_TIMER_TEST_CHILD";

/// Printed only after the assertions have run, so a skip cannot be mistaken for a pass.
const char * const child_sentinel = "THREAD_FUZZER_TIMER_ASSERTIONS_RAN";

/// The period is also the initial `it_value`, which counts down remaining process CPU time: a
/// period of a few microseconds could be read as 0 while the timer is still armed.
const char * const child_env[] = {
    "THREAD_FUZZER_CPU_TIME_PERIOD_US=100000",
    "THREAD_FUZZER_SLEEP_PROBABILITY=0.01",
    "THREAD_FUZZER_SLEEP_TIME_US_MAX=100000",
    "THREAD_FUZZER_EXPLICIT_MEMORY_EXCEPTION_PROBABILITY=1",
    "CH_THREAD_FUZZER_TIMER_TEST_CHILD=1",
};

struct ProfTimer
{
    /// Armed-ness lives here: `setitimer` disarms whenever `it_value` is 0, whatever `it_interval`
    /// holds, and `getitimer` reports a 0 `it_value` for an inactive timer.
    UInt64 value_us = 0;
    /// The reload amount, which decides only whether an armed timer fires more than once.
    UInt64 interval_us = 0;
};

UInt64 toMicroseconds(const struct timeval & tv)
{
    return static_cast<UInt64>(tv.tv_sec) * 1000000 + static_cast<UInt64>(tv.tv_usec);
}

/// A `getitimer` error fails the test here, because a zeroed result would reach the caller as a
/// disarmed timer.
ProfTimer profTimer()
{
    struct itimerval current{};
    if (0 != getitimer(ITIMER_PROF, &current))
        ADD_FAILURE() << "getitimer(ITIMER_PROF) failed: " << errnoToString();

    ProfTimer result;
    result.value_us = toMicroseconds(current.it_value);
    result.interval_us = toMicroseconds(current.it_interval);
    return result;
}

bool injectsMemoryLimitException()
{
    try
    {
        DB::ThreadFuzzer::maybeInjectMemoryLimitException();
    }
    catch (const DB::Exception & e)
    {
        return e.code() == DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED;
    }
    return false;
}

}

TEST(ThreadFuzzerTimer, StopStartTogglePerturbationChild)
{
    if (nullptr == getenv(child_marker_env)) // NOLINT(concurrency-mt-unsafe)
        GTEST_SKIP() << "runs only in the image exec'd by ThreadFuzzerTimer.StopStartTogglePerturbation";

    ASSERT_TRUE(DB::ThreadFuzzer::instance().isEffective());

    DB::ThreadFuzzer::instance().setup();
    ProfTimer timer = profTimer();
    EXPECT_NE(0UL, timer.value_us) << "setup() did not arm ITIMER_PROF";
    EXPECT_NE(0UL, timer.interval_us) << "setup() armed ITIMER_PROF to fire only once";
    EXPECT_TRUE(DB::ThreadFuzzer::isStarted());
    EXPECT_TRUE(injectsMemoryLimitException()) << "no fault injected while started";

    DB::ThreadFuzzer::stop();
    timer = profTimer();
    EXPECT_EQ(0UL, timer.value_us) << "stop() left ITIMER_PROF armed";
    EXPECT_EQ(0UL, timer.interval_us) << "stop() left a reload period on ITIMER_PROF";
    EXPECT_FALSE(injectsMemoryLimitException()) << "stop() did not stop fault injection";

    DB::ThreadFuzzer::start();
    timer = profTimer();
    EXPECT_NE(0UL, timer.value_us) << "start() did not re-arm ITIMER_PROF";
    EXPECT_NE(0UL, timer.interval_us) << "start() armed ITIMER_PROF to fire only once";
    EXPECT_TRUE(injectsMemoryLimitException()) << "start() did not resume fault injection";

    /// gtest teardown, the global Context destructor and the thread pool shutdowns that follow run
    /// with no perturbation armed. The parent asserts the exit code of that path.
    DB::ThreadFuzzer::stop();

    std::cout << child_sentinel << std::endl;
}

/// `getitimer` is not reachable from SQL, so the timer half of `SYSTEM STOP THREAD FUZZER` can only
/// be observed from a unit test.
TEST(ThreadFuzzerTimer, StopStartTogglePerturbation)
{
    /// The singleton reads the environment once, on first use, so a `fork` alone would inherit an
    /// already-built one: the configuration has to be present before the image starts.
    std::vector<char *> envp;
    for (char ** var = environ; *var; ++var)
    {
        /// Drop every inherited fuzzer knob, so the child's configuration is exactly this list.
        if (0 != strncmp(*var, "THREAD_FUZZER_", strlen("THREAD_FUZZER_"))
            && 0 != strncmp(*var, child_marker_env, strlen(child_marker_env)))
            envp.push_back(*var);
    }
    for (const char * var : child_env)
        envp.push_back(const_cast<char *>(var));
    envp.push_back(nullptr);

    char arg0[] = "unit_tests_dbms";
    char filter[] = "--gtest_filter=ThreadFuzzerTimer.StopStartTogglePerturbationChild";
    char * argv[] = {arg0, filter, nullptr};

    int fds[2];
    ASSERT_EQ(0, pipe(fds));

    pid_t pid = fork();
    ASSERT_NE(-1, pid);

    if (0 == pid)
    {
        /// Nothing here may allocate: another thread could have held the allocator at fork time.
        close(fds[0]);
        dup2(fds[1], STDOUT_FILENO);
        dup2(fds[1], STDERR_FILENO);
        close(fds[1]);
        execve("/proc/self/exe", argv, envp.data());
        _exit(127);
    }

    close(fds[1]);

    /// Drain before waiting: a child that fills the pipe would block forever otherwise.
    std::string output;
    char buf[4096];
    ssize_t got;
    while ((got = read(fds[0], buf, sizeof(buf))) > 0)
        output.append(buf, static_cast<size_t>(got));
    close(fds[0]);

    int status = 0;
    ASSERT_EQ(pid, waitpid(pid, &status, 0));

    EXPECT_TRUE(WIFEXITED(status) && 0 == WEXITSTATUS(status)) << "child failed, output:\n" << output;
    EXPECT_NE(std::string::npos, output.find(child_sentinel))
        << "child did not run its assertions, output:\n" << output;
}

/// `ITIMER_PROF` is process-wide and out of tree a CPU profiler owns it, so a process the fuzzer
/// configured no timer for must come out of `stop()` with its own timer intact.
TEST(ThreadFuzzerTimer, StopLeavesAForeignProfTimerAlone)
{
    /// The state under test is "the fuzzer configured no timer here". That predicate is private, but
    /// a configured timer makes the fuzzer effective, so a false `isEffective` establishes it.
    /// Asserted first: a future test exporting `THREAD_FUZZER_*` into this process would move this
    /// case onto the configured branch, where it would measure nothing.
    ASSERT_FALSE(DB::ThreadFuzzer::instance().isEffective())
        << "the fuzzer is configured in this process, so the unconfigured branch is not under test";
    ASSERT_EQ(0UL, profTimer().value_us) << "ITIMER_PROF was already armed on entry";

    /// Longer than this test can consume in process CPU time, so the timer cannot expire mid-test.
    static constexpr UInt64 foreign_interval_us = 7000000;

    const auto saved_disposition = std::signal(SIGPROF, SIG_IGN);
    ASSERT_NE(SIG_ERR, saved_disposition) << errnoToString();

    struct itimerval foreign{};
    foreign.it_interval.tv_sec = static_cast<time_t>(foreign_interval_us / 1000000);
    foreign.it_value.tv_sec = static_cast<time_t>(foreign_interval_us / 1000000);
    const int arm_rc = setitimer(ITIMER_PROF, &foreign, nullptr);
    ASSERT_EQ(0, arm_rc) << errnoToString();

    DB::ThreadFuzzer::stop();
    const ProfTimer after_stop = profTimer();

    /// Restored before the assertions, so a failure cannot leak the timer into later tests.
    struct itimerval disarm{};
    const int disarm_rc = setitimer(ITIMER_PROF, &disarm, nullptr);
    EXPECT_EQ(0, disarm_rc) << errnoToString();
    EXPECT_NE(SIG_ERR, std::signal(SIGPROF, saved_disposition)) << errnoToString();

    EXPECT_EQ(foreign_interval_us, after_stop.interval_us) << "stop() disarmed a timer it does not own";

    /// `it_value` counts down process CPU time, and the kernel reports the remainder rounded up to a
    /// whole timer tick, so it lands slightly above the period rather than exactly on it. A tick is
    /// at most 10 ms, so two of them bound the rounding on any supported HZ.
    static constexpr UInt64 tick_rounding_slack_us = 20000;
    EXPECT_GT(after_stop.value_us, foreign_interval_us / 2) << "stop() re-armed ITIMER_PROF";
    EXPECT_LE(after_stop.value_us, foreign_interval_us + tick_rounding_slack_us) << "stop() re-armed ITIMER_PROF";
}

#endif
