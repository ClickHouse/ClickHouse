#include <gtest/gtest.h>

#include <IO/preadNoWait.h>

#include <cerrno>

using namespace DB;


TEST(PreadNoWait, UnavailabilityIsRecognized)
{
    /// The system call is missing, the flag is not supported, or a `seccomp` profile rejects it.
    EXPECT_TRUE(isPreadNoWaitUnavailable(ENOSYS));
    EXPECT_TRUE(isPreadNoWaitUnavailable(EOPNOTSUPP));
    EXPECT_TRUE(isPreadNoWaitUnavailable(EPERM));

    /// These mean that the system call works, but this particular read did not happen.
    EXPECT_FALSE(isPreadNoWaitUnavailable(EAGAIN));
    EXPECT_FALSE(isPreadNoWaitUnavailable(EINTR));
    EXPECT_FALSE(isPreadNoWaitUnavailable(EBADF));
    EXPECT_FALSE(isPreadNoWaitUnavailable(EIO));
}

TEST(PreadNoWait, ProbeAcceptsOnlyEBADF)
{
    /// The probe passes an invalid file descriptor, so failing with `EBADF` is the only answer
    /// that proves the system call actually ran.
    EXPECT_FALSE(isPreadNoWaitProbeRejected(-1, EBADF));

    /// A `seccomp` filter can substitute an arbitrary `errno` (`SECCOMP_RET_ERRNO`), not just
    /// the ones the per-read classification knows, and even a fake success.
    EXPECT_TRUE(isPreadNoWaitProbeRejected(-1, EPERM));
    EXPECT_TRUE(isPreadNoWaitProbeRejected(-1, ENOSYS));
    EXPECT_TRUE(isPreadNoWaitProbeRejected(-1, EOPNOTSUPP));
    EXPECT_TRUE(isPreadNoWaitProbeRejected(-1, EACCES));
    EXPECT_TRUE(isPreadNoWaitProbeRejected(-1, EINVAL));
    EXPECT_TRUE(isPreadNoWaitProbeRejected(0, 0));
    EXPECT_TRUE(isPreadNoWaitProbeRejected(1, 0));
}

TEST(PreadNoWait, InvalidDescriptorIsRejected)
{
    /// An invalid file descriptor is the way the support is probed: the system call has to be
    /// dispatched before the descriptor is looked up, so this is what an available one answers.
    /// On a system where it is unavailable, the answer is whatever intercepted the system call
    /// (a `seccomp` filter can substitute an arbitrary result), so there is nothing to assert.
    char buf[1] = {};
    ssize_t res = preadNoWait(-1, buf, sizeof(buf), 0);
    if (preadNoWaitUnavailableReason().empty())
    {
        EXPECT_EQ(res, -1);
        EXPECT_EQ(errno, EBADF);
    }
}

TEST(PreadNoWait, TheReasonIsStable)
{
    /// The probe runs once, and the answer does not change while the process is running:
    /// `applySettingsQuirks` and `ThreadPoolReader` have to agree on it.
    EXPECT_EQ(preadNoWaitUnavailableReason(), preadNoWaitUnavailableReason());
}
