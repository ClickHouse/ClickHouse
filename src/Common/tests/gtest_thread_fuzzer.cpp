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

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <span>
#include <string>

#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <base/errnoToString.h>
#include <base/scope_guard.h>
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

/// Printed only after the assertions have run, so a skip cannot be mistaken for a pass. One per
/// child case, so a parent cannot be satisfied by another case's output.
const char * const child_sentinel = "THREAD_FUZZER_TIMER_ASSERTIONS_RAN";
const char * const timerless_child_sentinel = "THREAD_FUZZER_TIMERLESS_ASSERTIONS_RAN";
const char * const toggle_child_sentinel = "THREAD_FUZZER_CONCURRENT_TOGGLE_ASSERTIONS_RAN";

/// The period is also the initial `it_value`, which counts down remaining process CPU time: a
/// period of a few microseconds could be read as 0 while the timer is still armed.
const char * const child_env[] = {
    "THREAD_FUZZER_CPU_TIME_PERIOD_US=100000",
    "THREAD_FUZZER_SLEEP_PROBABILITY=0.01",
    "THREAD_FUZZER_SLEEP_TIME_US_MAX=100000",
    "THREAD_FUZZER_EXPLICIT_MEMORY_EXCEPTION_PROBABILITY=1",
    "CH_THREAD_FUZZER_TIMER_TEST_CHILD=1",
};

/// No CPU time period, so the fuzzer is effective through the memory-exception channel alone and
/// owns no `ITIMER_PROF`. `needsSetup` is false here while `isEffective` is true: the two toggle
/// halves are gated on different predicates, and this configuration separates them.
const char * const timerless_child_env[] = {
    "THREAD_FUZZER_EXPLICIT_MEMORY_EXCEPTION_PROBABILITY=1",
    "CH_THREAD_FUZZER_TIMER_TEST_CHILD=1",
};

/// A period far longer than this case can consume in process CPU time, so the timer never expires
/// and `it_value` stays a reliable armed-ness reading throughout. The yield probability is the
/// cheapest way to make `needsSetup` true; nothing here injects a sleep.
const char * const toggle_child_env[] = {
    "THREAD_FUZZER_CPU_TIME_PERIOD_US=30000000",
    "THREAD_FUZZER_YIELD_PROBABILITY=0.001",
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

/// Read back from the environment the parent exported, not repeated as a literal, so the period a
/// case expects cannot drift from the period it configured.
UInt64 configuredCpuTimePeriodUs()
{
    const char * env = getenv("THREAD_FUZZER_CPU_TIME_PERIOD_US"); // NOLINT(concurrency-mt-unsafe)
    return env ? static_cast<UInt64>(strtoull(env, nullptr, 10)) : 0;
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

void expectForeignTimerIntact(const ProfTimer & timer, UInt64 armed_interval_us, const char * after)
{
    EXPECT_EQ(armed_interval_us, timer.interval_us) << after << " disarmed a timer it does not own";

    /// `it_value` counts down process CPU time, and the kernel reports the remainder rounded up to a
    /// whole timer tick, so it lands slightly above the period rather than exactly on it. A tick is
    /// at most 10 ms, so two of them bound the rounding on any supported HZ.
    static constexpr UInt64 tick_rounding_slack_us = 20000;
    EXPECT_GT(timer.value_us, armed_interval_us / 2) << after << " re-armed ITIMER_PROF";
    EXPECT_LE(timer.value_us, armed_interval_us + tick_rounding_slack_us) << after << " re-armed ITIMER_PROF";
}

/// Runs one child case in a fresh image of this binary. The singleton reads the environment once, on
/// first use, so a `fork` alone would inherit an already-built one: the configuration has to be
/// present before the image starts.
void runChildCase(std::span<const char * const> child_env_vars, const char * child_case, const char * sentinel)
{
    std::vector<char *> envp;
    for (char ** var = environ; *var; ++var)
    {
        /// Drop every inherited fuzzer knob, so the child's configuration is exactly this list.
        if (0 != strncmp(*var, "THREAD_FUZZER_", strlen("THREAD_FUZZER_"))
            && 0 != strncmp(*var, child_marker_env, strlen(child_marker_env)))
            envp.push_back(*var);
    }
    for (const char * var : child_env_vars)
        envp.push_back(const_cast<char *>(var));
    envp.push_back(nullptr);

    char arg0[] = "unit_tests_dbms";
    std::string filter = std::string("--gtest_filter=") + child_case;
    char * argv[] = {arg0, filter.data(), nullptr};

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
    ssize_t got = 0;
    while ((got = read(fds[0], buf, sizeof(buf))) > 0)
        output.append(buf, static_cast<size_t>(got));
    close(fds[0]);

    int status = 0;
    ASSERT_EQ(pid, waitpid(pid, &status, 0));

    EXPECT_TRUE(WIFEXITED(status) && 0 == WEXITSTATUS(status)) << "child failed, output:\n" << output;
    EXPECT_NE(std::string::npos, output.find(sentinel))
        << "child did not run its assertions, output:\n" << output;
}

}

TEST(ThreadFuzzerTimer, StopStartTogglePerturbationChild)
{
    if (nullptr == getenv(child_marker_env)) // NOLINT(concurrency-mt-unsafe)
        GTEST_SKIP() << "runs only in the image exec'd by ThreadFuzzerTimer.StopStartTogglePerturbation";

    ASSERT_TRUE(DB::ThreadFuzzer::instance().isEffective());

    /// The reload period, asserted rather than merely found nonzero: a re-arm at any other period
    /// changes the perturbation cadence while leaving a presence-only oracle green.
    const UInt64 configured_period_us = configuredCpuTimePeriodUs();
    ASSERT_NE(0UL, configured_period_us) << "the child image was given no CPU time period";

    DB::ThreadFuzzer::instance().setup();
    ProfTimer timer = profTimer();
    EXPECT_NE(0UL, timer.value_us) << "setup() did not arm ITIMER_PROF";
    EXPECT_EQ(configured_period_us, timer.interval_us) << "setup() armed ITIMER_PROF off the configured period";
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
    EXPECT_EQ(configured_period_us, timer.interval_us) << "start() re-armed ITIMER_PROF off the configured period";
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
    runChildCase(child_env, "ThreadFuzzerTimer.StopStartTogglePerturbationChild", child_sentinel);
}

TEST(ThreadFuzzerTimer, StopStartTimerlessConfigurationChild)
{
    if (nullptr == getenv(child_marker_env)) // NOLINT(concurrency-mt-unsafe)
        GTEST_SKIP() << "runs only in the image exec'd by ThreadFuzzerTimer.StopStartTimerlessConfiguration";

    ASSERT_TRUE(DB::ThreadFuzzer::instance().isEffective());
    ASSERT_EQ(0UL, profTimer().value_us) << "ITIMER_PROF was already armed on entry";
    EXPECT_TRUE(injectsMemoryLimitException()) << "a configuration with no timer did not start injecting";

    /// Longer than this test can consume in process CPU time, so the timer cannot expire mid-test.
    static constexpr UInt64 foreign_interval_us = 7000000;

    const auto saved_disposition = std::signal(SIGPROF, SIG_IGN);
    ASSERT_NE(SIG_ERR, saved_disposition) << errnoToString();

    /// Every exit path restores both, including the early return of a failed assertion below, so no
    /// later test in this binary inherits the timer or the disposition.
    const auto restore_timer_state = make_scope_guard([&]
    {
        struct itimerval disarm{};
        const int disarm_rc = setitimer(ITIMER_PROF, &disarm, nullptr);
        EXPECT_EQ(0, disarm_rc) << errnoToString();
        const auto restored = std::signal(SIGPROF, saved_disposition);
        EXPECT_NE(SIG_ERR, restored) << errnoToString();
    });

    struct itimerval foreign{};
    foreign.it_interval.tv_sec = static_cast<time_t>(foreign_interval_us / 1000000);
    foreign.it_value.tv_sec = static_cast<time_t>(foreign_interval_us / 1000000);
    const int arm_rc = setitimer(ITIMER_PROF, &foreign, nullptr);
    ASSERT_EQ(0, arm_rc) << errnoToString();

    DB::ThreadFuzzer::stop();
    expectForeignTimerIntact(profTimer(), foreign_interval_us, "stop()");
    EXPECT_FALSE(injectsMemoryLimitException()) << "stop() did not stop fault injection";

    DB::ThreadFuzzer::start();
    expectForeignTimerIntact(profTimer(), foreign_interval_us, "start()");
    EXPECT_TRUE(injectsMemoryLimitException()) << "start() did not resume fault injection";

    DB::ThreadFuzzer::stop();

    std::cout << timerless_child_sentinel << std::endl;
}

/// A configuration that asks only for memory-exception injection makes the fuzzer effective without
/// giving it a timer, so `stop` and `start` must move the injection channel while leaving an
/// `ITIMER_PROF` they never armed to whoever does own it.
TEST(ThreadFuzzerTimer, StopStartTimerlessConfiguration)
{
    runChildCase(timerless_child_env, "ThreadFuzzerTimer.StopStartTimerlessConfigurationChild", timerless_child_sentinel);
}

TEST(ThreadFuzzerTimer, ConcurrentToggleKeepsFlagAndTimerAgreeingChild)
{
    if (nullptr == getenv(child_marker_env)) // NOLINT(concurrency-mt-unsafe)
        GTEST_SKIP() << "runs only in the image exec'd by ThreadFuzzerTimer.ConcurrentToggleKeepsFlagAndTimerAgreeing";

    ASSERT_TRUE(DB::ThreadFuzzer::instance().isEffective());

    /// Installs the `SIGPROF` handler, which a re-arm from `start` relies on already being there.
    DB::ThreadFuzzer::instance().setup();
    ASSERT_NE(0UL, profTimer().value_us) << "setup() did not arm ITIMER_PROF";

    /// Leaves the image in a known state on every exit path, including a failed assertion.
    const auto leave_stopped = make_scope_guard([] { DB::ThreadFuzzer::stop(); });

    /// A round is one `stop` racing one `start`, and only its final state is observable, so the round
    /// count is what buys detection probability. Rendezvousing two long-lived threads keeps a round
    /// at a few microseconds, which is why the count can be this high.
    static constexpr size_t rounds = 20000;

    std::atomic<size_t> go = 0;
    std::atomic<size_t> finished = 0;

    auto toggler = [&](void (*toggle)())
    {
        for (size_t round = 1; round <= rounds; ++round)
        {
            while (go.load(std::memory_order_acquire) < round)
            {
            }
            toggle();
            finished.fetch_add(1, std::memory_order_release);
        }
    };

    std::thread stopper(toggler, &DB::ThreadFuzzer::stop);
    std::thread starter(toggler, &DB::ThreadFuzzer::start);

    size_t torn = 0;
    size_t first_torn_round = 0;
    bool first_torn_started = false;

    for (size_t round = 1; round <= rounds; ++round)
    {
        finished.store(0, std::memory_order_relaxed);
        go.store(round, std::memory_order_release);
        while (finished.load(std::memory_order_acquire) < 2)
        {
        }

        /// Both togglers are parked on `go` here, so this reads the pair at rest: an inconsistency
        /// found now is a state the process was left in, not a torn view of a live update.
        const bool started = DB::ThreadFuzzer::isStarted();
        const bool armed = 0 != profTimer().value_us;
        if (started != armed)
        {
            if (0 == torn)
            {
                first_torn_round = round;
                first_torn_started = started;
            }
            ++torn;
        }
    }

    stopper.join();
    starter.join();

    EXPECT_EQ(0UL, torn) << "isStarted() and ITIMER_PROF disagreed after " << torn << " of " << rounds
                         << " rounds; first at round " << first_torn_round << ", where isStarted() was "
                         << first_torn_started << " and the timer was " << !first_torn_started;

    std::cout << toggle_child_sentinel << std::endl;
}

/// `stop` writes the flag before the timer and `start` writes the timer before the flag, so without
/// one lock over both an interleaving leaves the flag set with the timer disarmed. That state
/// persists, which is what makes it observable once the togglers are at rest.
TEST(ThreadFuzzerTimer, ConcurrentToggleKeepsFlagAndTimerAgreeing)
{
    runChildCase(
        toggle_child_env, "ThreadFuzzerTimer.ConcurrentToggleKeepsFlagAndTimerAgreeingChild", toggle_child_sentinel);
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

    /// Every exit path restores both, including the early return of a failed assertion below, so no
    /// later test in this binary inherits the timer or the disposition.
    const auto restore_timer_state = make_scope_guard([&]
    {
        struct itimerval disarm{};
        const int disarm_rc = setitimer(ITIMER_PROF, &disarm, nullptr);
        EXPECT_EQ(0, disarm_rc) << errnoToString();
        const auto restored = std::signal(SIGPROF, saved_disposition);
        EXPECT_NE(SIG_ERR, restored) << errnoToString();
    });

    struct itimerval foreign{};
    foreign.it_interval.tv_sec = static_cast<time_t>(foreign_interval_us / 1000000);
    foreign.it_value.tv_sec = static_cast<time_t>(foreign_interval_us / 1000000);
    const int arm_rc = setitimer(ITIMER_PROF, &foreign, nullptr);
    ASSERT_EQ(0, arm_rc) << errnoToString();

    DB::ThreadFuzzer::stop();
    expectForeignTimerIntact(profTimer(), foreign_interval_us, "stop()");
}

#endif
