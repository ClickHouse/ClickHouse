#include <Common/SignalHandlers.h>

#include <base/defines.h>

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#if defined(OS_HAS_SIGNAL_HANDLERS)

#include <Poco/SignalHandler.h>
#include <Poco/Util/Application.h>

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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"

[[noreturn]] void checkResetRestoresPreviousActions()
{
    /// Everything `setupCommonDeadlySignalHandlers` registers.
    const std::vector<int> registered{SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGTSTP, SIGTRAP};

    /// Stands for a handler installed before ours, like a sanitizer runtime's.
    struct sigaction sentinel{};
    sentinel.sa_sigaction = sentinelHandler;
    sentinel.sa_flags = SA_SIGINFO;
    require(sigemptyset(&sentinel.sa_mask) == 0, "sigemptyset");
    for (int sig : registered)
        require(sigaction(sig, &sentinel, nullptr) == 0, "installing the sentinel for signal " + std::to_string(sig));

    /// Registered twice, so only the first registration replaced the sentinel.
    HandledSignals::instance().setupCommonDeadlySignalHandlers();
    HandledSignals::instance().setupCommonDeadlySignalHandlers();

    for (int sig : registered)
    {
        struct sigaction installed{};
        require(sigaction(sig, nullptr, &installed) == 0, "reading the installed action of signal " + std::to_string(sig));
        require(installed.sa_sigaction != sentinelHandler, "the sentinel was replaced, signal " + std::to_string(sig));
    }

    HandledSignals::instance().reset(/* close_pipe= */ false);

    for (int sig : registered)
    {
        const std::string suffix = ", signal " + std::to_string(sig);
        struct sigaction after{};
        require(sigaction(sig, nullptr, &after) == 0, "reading the restored action" + suffix);
        require(after.sa_sigaction == sentinelHandler, "the previous handler is back" + suffix);
        require((after.sa_flags & SA_SIGINFO) != 0, "SA_SIGINFO is back" + suffix);
    }

    std::_Exit(0);
}

struct PocoSignalHandler : Poco::SignalHandler
{
    static auto address() { return &handleSignal; }
};

[[noreturn]] void checkApplicationKeepsFaultActions()
{
    /// At most one may exist; leaked, the child ends with `_Exit`.
    if (Poco::Util::Application::instanceRawPtr() == nullptr)
        new Poco::Util::Application;

    for (int sig : {SIGSEGV, SIGBUS, SIGILL, SIGSYS})
    {
        const std::string suffix = ", signal " + std::to_string(sig);
        struct sigaction current{};
        require(sigaction(sig, nullptr, &current) == 0, "reading the action" + suffix);
        require(current.sa_handler != PocoSignalHandler::address(), "the Poco handler is not installed" + suffix);
    }

    std::_Exit(0);
}

#pragma clang diagnostic pop

}

/// Runs in a child process: it installs the real handlers, which must not survive into the rest of
/// the test binary.
TEST(SignalHandlersDeathTest, ResetRestoresPreviousActions)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkResetRestoresPreviousActions(), ::testing::ExitedWithCode(0), ".*");
}

TEST(SignalHandlersDeathTest, ApplicationKeepsFaultActions)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkApplicationKeepsFaultActions(), ::testing::ExitedWithCode(0), ".*");
}

#endif
