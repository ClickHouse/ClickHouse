#if defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Common/TimerDescriptor.h>
#include <base/scope_guard.h>

#include <poll.h>
#include <unistd.h>
#include <sys/resource.h>

#include <cerrno>
#include <chrono>
#include <thread>

namespace
{

bool isReadable(int fd)
{
    pollfd poll_fd{};
    poll_fd.fd = fd;
    poll_fd.events = POLLIN;
    int res = ::poll(&poll_fd, 1, 0);
    EXPECT_GE(res, 0);
    return res > 0;
}

/// Arm a short timer and wait for it to fire.
void armAndAwait(const DB::TimerDescriptor & timer)
{
    timer.setRelative(static_cast<uint64_t>(1000));
    for (int i = 0; i < 1000 && !isReadable(timer.getDescriptor()); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(isReadable(timer.getDescriptor())) << "timer never fired";
}

}

TEST(TimerDescriptor, ResetConsumesExpiration)
{
    DB::TimerDescriptor timer;

    /// Not armed: reset is a no-op, must not throw and must be idempotent.
    ASSERT_NO_THROW(timer.reset());
    ASSERT_NO_THROW(timer.reset());
    ASSERT_FALSE(isReadable(timer.getDescriptor()));

    armAndAwait(timer);

    ASSERT_NO_THROW(timer.reset());
    ASSERT_FALSE(isReadable(timer.getDescriptor())) << "reset did not drain the expiration";
}

/// `reset` must not need a new file descriptor. It used to build a temporary `Epoll` to poll the
/// timer, so under descriptor exhaustion it threw and left its callers half torn down --
/// see `RemoteQueryExecutorReadContext::clearAsyncEvent`.
TEST(TimerDescriptor, ResetSurvivesFileDescriptorExhaustion)
{
    DB::TimerDescriptor timer;
    armAndAwait(timer);

    /// glibc defines `RLIMIT_NOFILE` as the recursive macro `RLIMIT_NOFILE`, which trips
    /// `-Wdisabled-macro-expansion` when used as a function argument.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    rlimit old_limit{};
    ASSERT_EQ(0, ::getrlimit(RLIMIT_NOFILE, &old_limit));

    /// Find the lowest free descriptor number and clamp the soft limit to it. Descriptors that are
    /// already open stay valid, but every new allocation fails with EMFILE. Much cheaper and less
    /// invasive than actually opening every descriptor up to the limit.
    int probe = ::dup(timer.getDescriptor());
    ASSERT_NE(-1, probe);
    ASSERT_EQ(0, ::close(probe));

    rlimit new_limit = old_limit;
    new_limit.rlim_cur = static_cast<rlim_t>(probe);
    if (0 != ::setrlimit(RLIMIT_NOFILE, &new_limit))
        GTEST_SKIP() << "Cannot lower RLIMIT_NOFILE";

    SCOPE_EXIT({ ::setrlimit(RLIMIT_NOFILE, &old_limit); });
#pragma clang diagnostic pop

    ASSERT_EQ(-1, ::dup(timer.getDescriptor())) << "descriptors are still available, test is not testing anything";
    ASSERT_EQ(EMFILE, errno);

    ASSERT_NO_THROW(timer.reset());
    ASSERT_FALSE(isReadable(timer.getDescriptor())) << "reset did not drain the expiration";
}

#endif
