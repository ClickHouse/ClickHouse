#include <Common/WakeupFd.h>

#include <base/defines.h>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

using namespace DB;

TEST(WakeupFd, NotifyDrainDoesNotBlock)
{
    WakeupFd wake;

    /// Empty pipe: drain must return immediately (non-blocking read end).
    wake.drain();

    /// Multiple notifies collapse into "readable"; drain consumes them all and returns.
    wake.notify();
    wake.notify();
    wake.drain();
    wake.drain();
}

/// The fd validation is compiled only into debug/sanitizer builds, where a LOGICAL_ERROR aborts,
/// so the detection cases are death tests. Fork-based death tests are unreliable under TSan.
#if defined(DEBUG_OR_SANITIZER_BUILD) && !defined(THREAD_SANITIZER)

TEST(WakeupFdDeathTest, DetectsLostNonBlocking)
{
    WakeupFd wake;

    int read_fd = wake.fd();
    int flags = fcntl(read_fd, F_GETFL);
    ASSERT_NE(flags, -1);
    ASSERT_TRUE(flags & O_NONBLOCK);

    /// Simulate foreign code stripping O_NONBLOCK from our fd: drain must fail loudly, not block.
    ASSERT_EQ(0, fcntl(read_fd, F_SETFL, flags & ~O_NONBLOCK));
    EXPECT_DEATH(wake.drain(), "");

    ASSERT_EQ(0, fcntl(read_fd, F_SETFL, flags));
    wake.drain();
}

TEST(WakeupFdDeathTest, DetectsRecycledFd)
{
    WakeupFd wake;

    /// Simulate a foreign double close: our read fd number is closed and recycled by an unrelated
    /// open. POSIX guarantees open() returns the lowest free descriptor, so it reuses the number.
    int read_fd = wake.fd();
    ASSERT_EQ(0, ::close(read_fd));
    int recycled = ::open("/dev/null", O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    ASSERT_EQ(recycled, read_fd);

    EXPECT_DEATH(wake.drain(), "");
    /// Do not close `recycled` here: WakeupFd still believes it owns the number and closes it.
}

#endif
