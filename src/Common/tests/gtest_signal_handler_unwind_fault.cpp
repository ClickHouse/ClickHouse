#include <Common/SignalHandlers.h>

#include <base/defines.h>

#include <gtest/gtest.h>

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

#include <sys/mman.h>
#include <unistd.h>

#if defined(OS_LINUX)

namespace
{

/// A failed gtest assertion in the child would not reach the parent, which sees only the exit code.
void require(bool condition, const std::string & what)
{
    if (!condition)
    {
        std::cerr << "failed: " << what << '\n';
        std::_Exit(1);
    }
}

/// While SIGTSTP is handled, the caller's saved frame pointer points at `unmapped`, so the handler's
/// walk faults when it unwinds the caller.
[[gnu::noinline]] void raiseWithCorruptFrameChain(void * unmapped)
{
    auto * frame = static_cast<void * volatile *>(__builtin_frame_address(0));
    void * saved = frame[0];
    frame[0] = unmapped;
    const int raised = raise(SIGTSTP);
    frame[0] = saved;
    require(raised == 0, "raising SIGTSTP");
}

/// Taking the frame address makes the CFA of this frame frame-pointer based in every build mode.
[[gnu::noinline, clang::disable_tail_calls]] void callThroughFramePointerFrame(void * unmapped)
{
    void * volatile frame = __builtin_frame_address(0);
    raiseWithCorruptFrameChain(unmapped);
    (void)frame;
}

[[noreturn]] void checkUnwindFaultInBlockingHandlerIsRecovered()
{
    HandledSignals::instance().setupCommonDeadlySignalHandlers();
    HandledSignals::instance().signal_pipe.setNonBlockingRead();

    constexpr size_t region_size = 1 << 20;
    void * region = mmap(nullptr, region_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    require(region != MAP_FAILED, "mapping the inaccessible region");

    callThroughFramePointerFrame(static_cast<char *>(region) + region_size / 2);

    char report[signal_pipe_buf_size];
    const ssize_t reported = ::read(HandledSignals::instance().signal_pipe.fds_rw[0], report, sizeof(report));
    require(reported > static_cast<ssize_t>(sizeof(int) + sizeof(siginfo_t)), "a fault record was reported");

    std::_Exit(0);
}

}

/// The SIGTSTP handler blocks the fault signals, like the deadly handler. A fault in its own stack walk
/// must be recovered and the record still reported, instead of killing the process.
TEST(SignalHandlersDeathTest, UnwindFaultInBlockingHandlerIsRecovered)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkUnwindFaultInBlockingHandlerIsRecovered(), ::testing::ExitedWithCode(0), ".*");
}

#endif
