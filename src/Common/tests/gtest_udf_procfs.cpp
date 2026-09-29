#if defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Common/UDFProcessSubtreeSampler.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace DB;


TEST(UDFProcfs, ReadCurrentRssRejectsInvalidPid)
{
    UInt64 bytes = 123;
    EXPECT_FALSE(UDFProcfs::readCurrentRss(0, bytes));
    EXPECT_EQ(bytes, 0u);

    bytes = 123;
    EXPECT_FALSE(UDFProcfs::readCurrentRss(-1, bytes));
    EXPECT_EQ(bytes, 0u);
}


TEST(UDFProcfs, ReadCurrentRssOfSelfIsPositive)
{
    UInt64 bytes = 0;
    ASSERT_TRUE(UDFProcfs::readCurrentRss(::getpid(), bytes));
    EXPECT_GT(bytes, 0u);
}


TEST(UDFProcfs, IsZombieRejectsInvalidPid)
{
    EXPECT_FALSE(UDFProcfs::isZombie(0));
    EXPECT_FALSE(UDFProcfs::isZombie(-1));
}


TEST(UDFProcfs, IsZombieIsFalseForRunningSelf)
{
    EXPECT_FALSE(UDFProcfs::isZombie(::getpid()));
}


TEST(UDFProcfs, IsZombieDetectsZombieWhichHasNoRss)
{
    pid_t child = ::fork();
    ASSERT_GE(child, 0) << "fork failed";
    if (child == 0)
        _exit(0); /// Child exits immediately; only async-signal-safe `_exit` is used.

    /// Block until the child has exited but leave it unreaped (a zombie):
    siginfo_t info{};
    ASSERT_EQ(::waitid(P_PID, static_cast<id_t>(child), &info, WEXITED | WNOWAIT), 0) << "waitid failed";

    /// A zombie has released its address space, so `/proc/<pid>/status` has no
    /// `VmRSS` line and `readCurrentRss` fails.
    EXPECT_TRUE(UDFProcfs::isZombie(child));
    UInt64 bytes = 123;
    EXPECT_FALSE(UDFProcfs::readCurrentRss(child, bytes));
    EXPECT_EQ(bytes, 0u);

    /// Reap it; afterwards `/proc/<pid>` is gone, so neither helper reports it.
    ASSERT_EQ(::waitpid(child, nullptr, 0), child);
    EXPECT_FALSE(UDFProcfs::isZombie(child));
    bytes = 123;
    EXPECT_FALSE(UDFProcfs::readCurrentRss(child, bytes));
}

#endif
