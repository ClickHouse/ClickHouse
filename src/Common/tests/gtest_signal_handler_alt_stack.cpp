#include <Common/SignalHandlers.h>

#include <base/defines.h>
#include <Common/StackTraceServiceSignal.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include <unistd.h>

#if defined(OS_HAS_SIGNAL_HANDLERS)

namespace
{

void noopHandler(int, siginfo_t *, void *)
{
}

/// A failed gtest assertion in the child would not reach the parent, which sees only the exit code.
void require(bool condition, const std::string & what)
{
    if (!condition)
    {
        std::cerr << "failed: " << what << '\n';
        std::_Exit(1);
    }
}

[[noreturn]] void checkAlternativeStackIsRequested()
{
    /// Everything `setupCommonDeadlySignalHandlers` registers.
    const std::vector<int> registered{SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGTSTP, SIGTRAP};

    /// Both registrations mask everything the single pre-split registration masked.
    std::vector<int> expected_mask = registered;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    for (int sig : {SIGUSR1, SIGUSR2, DB::STACK_TRACE_SERVICE_SIGNAL})
        expected_mask.push_back(sig);
#endif

    HandledSignals::instance().setupCommonDeadlySignalHandlers();

    struct sigaction reference{};
    require(sigaction(SIGSEGV, nullptr, &reference) == 0, "reading the SIGSEGV action");
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    require(reference.sa_sigaction != nullptr, "SIGSEGV has a handler");

    for (int sig : registered)
    {
        const std::string suffix = ", signal " + std::to_string(sig);
        struct sigaction actual{};
        require(sigaction(sig, nullptr, &actual) == 0, "reading the action" + suffix);
        /// SIGTSTP is the one whose handler returns, so it stays off the alternative stack.
        require(bool(actual.sa_flags & SA_ONSTACK) == (sig != SIGTSTP), "SA_ONSTACK" + suffix);
        /// `SA_ONSTACK` must be the only difference the split introduces.
        require(actual.sa_sigaction == reference.sa_sigaction, "same handler" + suffix);
        require((actual.sa_flags & ~SA_ONSTACK) == (reference.sa_flags & ~SA_ONSTACK), "same flags" + suffix);
        for (int masked = 1; masked < NSIG; ++masked)
        {
            const bool expected = std::find(expected_mask.begin(), expected_mask.end(), masked) != expected_mask.end();
            require((sigismember(&actual.sa_mask, masked) == 1) == expected, "mask member " + std::to_string(masked) + suffix);
        }
    }

    /// Anchors the identity above to the fault handler: only it reports a whole fault record, so
    /// rewiring both registrations to any other handler would leave the comparisons above intact.
    HandledSignals::instance().signal_pipe.setNonBlockingRead();
    require(raise(SIGTSTP) == 0, "raising SIGTSTP");
    char report[signal_pipe_buf_size];
    const ssize_t reported = ::read(HandledSignals::instance().signal_pipe.fds_rw[0], report, sizeof(report));
    require(reported > static_cast<ssize_t>(sizeof(int) + sizeof(siginfo_t)), "a fault record was reported");

    /// `use_alt_stack` is opt-in: an ordinary caller using the default must not acquire the flag.
    HandledSignals::instance().addSignalHandler({SIGSYS}, noopHandler, false);
    struct sigaction defaulted{};
    require(sigaction(SIGSYS, nullptr, &defaulted) == 0, "reading the defaulted action");
    require(!(defaulted.sa_flags & SA_ONSTACK), "the default is off the alternative stack");
#pragma clang diagnostic pop

    std::_Exit(0);
}

}

/// Runs in a child process: it installs the real handlers, which must not survive into the rest of
/// the test binary.
TEST(SignalHandlersDeathTest, DeadlySignalHandlersRequestAlternativeStack)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkAlternativeStackIsRequested(), ::testing::ExitedWithCode(0), ".*");
}

#endif
