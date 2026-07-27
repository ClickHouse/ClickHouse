#include <Common/WakeupFd.h>
#include <Common/Exception.h>

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

/// The fd validation is compiled only into debug/sanitizer builds. The tests use the non-throwing
/// checkEnd: calling drain/notify on a tampered fd would abort (LOGICAL_ERROR in these builds).
#ifdef DEBUG_OR_SANITIZER_BUILD

TEST(WakeupFd, DetectsLostNonBlocking)
{
    WakeupFd wake;
    EXPECT_FALSE(wake.checkEnd(0).has_value());
    EXPECT_FALSE(wake.checkEnd(1).has_value());

    int read_fd = wake.fd();
    int flags = fcntl(read_fd, F_GETFL);
    ASSERT_NE(flags, -1);
    ASSERT_TRUE(flags & O_NONBLOCK);

    /// Simulate foreign code stripping O_NONBLOCK from our fd.
    ASSERT_EQ(0, fcntl(read_fd, F_SETFL, flags & ~O_NONBLOCK));
    auto problem = wake.checkEnd(0);
    ASSERT_TRUE(problem.has_value());
    EXPECT_TRUE(problem->text.contains("lost O_NONBLOCK"));

    ASSERT_EQ(0, fcntl(read_fd, F_SETFL, flags));
    EXPECT_FALSE(wake.checkEnd(0).has_value());
    wake.drain();
}

TEST(WakeupFd, DetectsRecycledFd)
{
    WakeupFd wake;

    /// Simulate a foreign double close: our read fd number is closed and recycled by an unrelated
    /// open. POSIX guarantees open() returns the lowest free descriptor, so it reuses the number.
    int read_fd = wake.fd();
    ASSERT_EQ(0, ::close(read_fd));
    int recycled = ::open("/dev/null", O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    ASSERT_EQ(recycled, read_fd);

    auto problem = wake.checkEnd(0);
    ASSERT_TRUE(problem.has_value());
    EXPECT_TRUE(problem->text.contains("refers to another file"));
    /// Do not drain() here (it would abort on the problem) and do not close `recycled`:
    /// WakeupFd still believes it owns the number and closes it.
}

#endif
