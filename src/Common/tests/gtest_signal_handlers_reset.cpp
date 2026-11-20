#include <Common/SignalHandlers.h>

#include <base/defines.h>

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#if defined(OS_HAS_SIGNAL_HANDLERS)

namespace
{

void sentinelHandler(int, siginfo_t *, void *)
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

[[noreturn]] void checkResetRestoresPreviousActions()
{
    /// Everything `setupCommonDeadlySignalHandlers` registers.
    const std::vector<int> registered{SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGTSTP, SIGTRAP};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    /// Stands for a handler installed before ours, like a sanitizer runtime's.
    struct sigaction sentinel{};
    sentinel.sa_sigaction = sentinelHandler;
    sentinel.sa_flags = SA_SIGINFO;
    require(sigemptyset(&sentinel.sa_mask) == 0, "sigemptyset");
    require(sigaction(SIGTRAP, &sentinel, nullptr) == 0, "installing the sentinel");

    std::vector<struct sigaction> before(registered.size());
    for (size_t i = 0; i < registered.size(); ++i)
        require(sigaction(registered[i], nullptr, &before[i]) == 0, "reading the action of signal " + std::to_string(registered[i]));

    HandledSignals::instance().setupCommonDeadlySignalHandlers();

    struct sigaction installed{};
    require(sigaction(SIGTRAP, nullptr, &installed) == 0, "reading the installed SIGTRAP action");
    require(installed.sa_sigaction != sentinelHandler, "the sentinel was replaced");

    HandledSignals::instance().reset(/* close_pipe= */ false);

    for (size_t i = 0; i < registered.size(); ++i)
    {
        const std::string suffix = ", signal " + std::to_string(registered[i]);
        struct sigaction after{};
        require(sigaction(registered[i], nullptr, &after) == 0, "reading the restored action" + suffix);
        require(after.sa_sigaction == before[i].sa_sigaction, "the previous handler is back" + suffix);
        require((after.sa_flags & SA_SIGINFO) == (before[i].sa_flags & SA_SIGINFO), "SA_SIGINFO is back" + suffix);
    }
#pragma clang diagnostic pop

    std::_Exit(0);
}

}

/// Runs in a child process: it installs the real handlers, which must not survive into the rest of
/// the test binary.
TEST(SignalHandlersDeathTest, ResetRestoresPreviousActions)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkResetRestoresPreviousActions(), ::testing::ExitedWithCode(0), ".*");
}

#endif
