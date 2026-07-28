#if defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Common/UDFProcessRegistry.h>

#include <csignal>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace DB;

/// A stale removal carrying the predecessor's generation must not evict a successor that reused the pid.
TEST(UDFProcessRegistry, StaleRemovalDoesNotEvictReusedPid)
{
    pid_t child = ::fork();
    ASSERT_GE(child, 0) << "fork failed";
    if (child == 0)
    {
        ::pause();
        _exit(0);
    }

    auto & reg = UDFProcessRegistry::instance();
    const UInt64 baseline = reg.sample().process_count;

    const UInt64 g1 = reg.add(child);
    const UInt64 g2 = reg.add(child); /// pid reused: predecessor reaped, same pid re-added
    EXPECT_NE(g1, g2) << "add() must hand out distinct generations";

    reg.removeIfGenerationMatches(child, g1);
    EXPECT_EQ(reg.sample().process_count, baseline + 1) << "stale-generation removal wrongly evicted the reused pid";

    reg.removeIfGenerationMatches(child, g2);
    EXPECT_EQ(reg.sample().process_count, baseline) << "current-generation removal should evict the entry";

    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
}

TEST(UDFProcessRegistry, RemoveIfGenerationMatchesUnknownPidIsNoOp)
{
    auto & reg = UDFProcessRegistry::instance();
    const UInt64 before = reg.sample().process_count;
    reg.removeIfGenerationMatches(987654, 1);
    EXPECT_EQ(reg.sample().process_count, before);
}

#endif
