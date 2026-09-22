#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Common/TimerDescriptor.h>

#include <poll.h>

namespace
{

bool waitReadable(int fd, int timeout_ms)
{
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    return ::poll(&descriptor, 1, timeout_ms) > 0 && (descriptor.revents & POLLIN);
}

}

/// Arming a timer must clear an expiration left by the previous arm, so that a caller reusing a
/// TimerDescriptor does not read the fresh timer as already alarmed and report an instant timeout.
TEST(TimerDescriptor, ArmClearsPendingExpiration)
{
    DB::TimerDescriptor timer;

    timer.setRelative(static_cast<uint64_t>(1'000));
    ASSERT_TRUE(waitReadable(timer.getDescriptor(), 10'000));

    timer.setRelative(static_cast<uint64_t>(10'000'000));
    EXPECT_FALSE(waitReadable(timer.getDescriptor(), 0));
}

#endif
