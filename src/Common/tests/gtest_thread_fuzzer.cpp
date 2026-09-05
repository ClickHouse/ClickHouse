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

#include <cstring>
#include <iostream>
#include <string>

#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

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

const char * const child_env[] = {
    "THREAD_FUZZER_CPU_TIME_PERIOD_US=1000",
    "THREAD_FUZZER_SLEEP_PROBABILITY=0.01",
    "THREAD_FUZZER_SLEEP_TIME_US_MAX=100000",
    "THREAD_FUZZER_EXPLICIT_MEMORY_EXCEPTION_PROBABILITY=1",
    "CH_THREAD_FUZZER_TIMER_TEST_CHILD=1",
};

UInt64 profTimerIntervalUs()
{
    struct itimerval current{};
    if (0 != getitimer(ITIMER_PROF, &current))
        return 0;
    return static_cast<UInt64>(current.it_interval.tv_sec) * 1000000 + current.it_interval.tv_usec;
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
    EXPECT_NE(0UL, profTimerIntervalUs()) << "setup() did not arm ITIMER_PROF";
    EXPECT_TRUE(DB::ThreadFuzzer::isStarted());
    EXPECT_TRUE(injectsMemoryLimitException()) << "no fault injected while started";

    DB::ThreadFuzzer::stop();
    EXPECT_EQ(0UL, profTimerIntervalUs()) << "stop() left ITIMER_PROF armed";
    EXPECT_FALSE(injectsMemoryLimitException()) << "stop() did not stop fault injection";

    DB::ThreadFuzzer::start();
    EXPECT_NE(0UL, profTimerIntervalUs()) << "start() did not re-arm ITIMER_PROF";
    EXPECT_TRUE(injectsMemoryLimitException()) << "start() did not resume fault injection";

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

#endif
