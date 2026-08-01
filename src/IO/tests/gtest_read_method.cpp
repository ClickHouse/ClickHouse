#include <gtest/gtest.h>

#include <IO/ReadMethod.h>
#include <IO/preadNoWait.h>

#include <cerrno>

using namespace DB;


TEST(ReadMethod, PreadThreadpoolNeedsPreadNoWait)
{
    /// The page cache check is available: the requested method is used as is.
    EXPECT_EQ(
        resolveLocalFSReadMethod(LocalFSReadMethod::pread_threadpool, /*pread_no_wait_supported*/ true, /*direct_io*/ false),
        LocalFSReadMethod::pread_threadpool);

    /// It is not available: the thread pool would be pure overhead, so the data is read in the calling thread.
    EXPECT_EQ(
        resolveLocalFSReadMethod(LocalFSReadMethod::pread_threadpool, /*pread_no_wait_supported*/ false, /*direct_io*/ false),
        LocalFSReadMethod::pread);

    /// O_DIRECT reads never look at the page cache and are always performed in the thread pool.
    EXPECT_EQ(
        resolveLocalFSReadMethod(LocalFSReadMethod::pread_threadpool, /*pread_no_wait_supported*/ false, /*direct_io*/ true),
        LocalFSReadMethod::pread_threadpool);
}

TEST(ReadMethod, OtherMethodsAreNotAffected)
{
    for (auto method : {LocalFSReadMethod::read, LocalFSReadMethod::pread, LocalFSReadMethod::mmap,
                        LocalFSReadMethod::io_uring, LocalFSReadMethod::pread_fake_async})
    {
        for (bool supported : {true, false})
        {
            for (bool direct_io : {true, false})
                EXPECT_EQ(resolveLocalFSReadMethod(method, supported, direct_io), method);
        }
    }
}

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

TEST(PreadNoWait, SupportIsReportedWithAReason)
{
    /// The probe result depends on the kernel and on the sandbox this test runs in,
    /// but a system that cannot use it always says why, for `system.warnings`.
    const auto & support = getPreadNoWaitSupport();
    EXPECT_EQ(support.supported, support.unsupported_reason.empty());
}

TEST(PreadNoWait, InvalidDescriptorIsRejected)
{
    /// An invalid file descriptor is the way the support is probed: the system call has to be
    /// dispatched before the descriptor is looked up, so this is what an available one answers.
    char buf[1] = {};
    EXPECT_EQ(preadNoWait(-1, buf, sizeof(buf), 0), -1);
    if (getPreadNoWaitSupport().supported)
        EXPECT_EQ(errno, EBADF);
    else
        EXPECT_TRUE(errno == EBADF || isPreadNoWaitUnavailable(errno));
}
