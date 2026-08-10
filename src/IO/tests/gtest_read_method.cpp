#include <gtest/gtest.h>

#include <IO/ReadMethod.h>
#include <IO/preadNoWait.h>
#include <base/unit.h>

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

TEST(ReadMethod, OnDemandOverloadDoesNotProbeWhenTheCheckIsNotNeeded)
{
    /// Resolutions that do not need the page cache check must not run the probe:
    /// it is a raw `preadv2` system call that a kill-on-deny `seccomp` profile terminates
    /// the process for. `isPreadNoWaitProbed` observes the one-time probe directly, so this
    /// holds regardless of whether an earlier test has already probed.
    const bool probed_before = isPreadNoWaitProbed();

    /// Another read method never reaches the probe...
    for (auto method : {LocalFSReadMethod::read, LocalFSReadMethod::pread, LocalFSReadMethod::mmap,
                        LocalFSReadMethod::io_uring, LocalFSReadMethod::pread_fake_async})
    {
        for (bool direct_io : {true, false})
            EXPECT_EQ(resolveLocalFSReadMethod(method, direct_io), method);
    }

    /// ...and neither does an O_DIRECT read, which never looks at the page cache
    /// and keeps the thread pool.
    EXPECT_EQ(
        resolveLocalFSReadMethod(LocalFSReadMethod::pread_threadpool, /*direct_io*/ true),
        LocalFSReadMethod::pread_threadpool);

    EXPECT_EQ(isPreadNoWaitProbed(), probed_before);
}

TEST(ReadMethod, OnDemandOverloadProbesOnlyForPreadThreadpool)
{
    /// A non-O_DIRECT 'pread_threadpool' read is the one case that needs the page cache check,
    /// so it resolves from the probed support.
    EXPECT_EQ(
        resolveLocalFSReadMethod(LocalFSReadMethod::pread_threadpool, /*direct_io*/ false),
        resolveLocalFSReadMethod(
            LocalFSReadMethod::pread_threadpool, getPreadNoWaitSupport().supported, /*direct_io*/ false));

    EXPECT_TRUE(isPreadNoWaitProbed());
}

TEST(ReadMethod, DirectIOBasisMatchesTheReader)
{
    /// A zero threshold disables O_DIRECT, and a read smaller than the threshold does not reach it.
    EXPECT_FALSE(willUseDirectIO(/*estimated_size*/ 1_GiB, /*direct_io_threshold*/ 0));
    EXPECT_FALSE(willUseDirectIO(/*estimated_size*/ 1_MiB - 1, /*direct_io_threshold*/ 1_MiB));

    /// A read at or above the threshold uses O_DIRECT — but only where the reader can:
    /// `createReadBufferFromFileBase` only attempts it under Linux and FreeBSD, so on any other
    /// platform the answer stays 'no' and 'pread_threadpool' keeps falling back to 'pread'.
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    constexpr bool direct_io_possible = true;
#else
    constexpr bool direct_io_possible = false;
#endif

    EXPECT_EQ(willUseDirectIO(/*estimated_size*/ 1_MiB, /*direct_io_threshold*/ 1_MiB), direct_io_possible);
    EXPECT_EQ(willUseDirectIO(/*estimated_size*/ 1_GiB, /*direct_io_threshold*/ 1_MiB), direct_io_possible);

    /// This is what the carriers that resolve the method before the buffer is created rely on:
    /// a large non-O_DIRECT-capable read must resolve exactly like a small one, so that
    /// `DiskLocal::prepareRead` and the reader never disagree about the method.
    for (size_t size : {size_t(0), 1_MiB - 1, 1_MiB, 1_GiB})
    {
        EXPECT_EQ(
            resolveLocalFSReadMethod(
                LocalFSReadMethod::pread_threadpool,
                /*pread_no_wait_supported*/ false,
                willUseDirectIO(size, /*direct_io_threshold*/ 1_MiB)),
            direct_io_possible && size >= 1_MiB ? LocalFSReadMethod::pread_threadpool : LocalFSReadMethod::pread);
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
    /// On a system where it is unavailable, the answer is whatever intercepted the system call
    /// (a `seccomp` filter can substitute an arbitrary result), so there is nothing to assert.
    char buf[1] = {};
    ssize_t res = preadNoWait(-1, buf, sizeof(buf), 0);
    if (getPreadNoWaitSupport().supported)
    {
        EXPECT_EQ(res, -1);
        EXPECT_EQ(errno, EBADF);
    }
}
