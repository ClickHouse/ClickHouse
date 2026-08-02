#include <gtest/gtest.h>

#include <Common/PerCPU.h>

#if defined(OS_LINUX)

#include <sched.h>

#include <thread>
#include <vector>

namespace
{

/// Pin the current thread to one CPU; verified via libc, so the tests cross-check two
/// independent ways of learning the current CPU.
bool pinTo(int cpu)
{
    cpu_set_t one;
    CPU_ZERO(&one);
    CPU_SET(cpu, &one);
    return sched_setaffinity(0, sizeof(one), &one) == 0 && sched_getcpu() == cpu;
}

/// CPUs this process may run on.
std::vector<int> allowedCpus()
{
    cpu_set_t set;
    std::vector<int> cpus;
    if (sched_getaffinity(0, sizeof(set), &set) != 0)
        return cpus;
    for (int c = 0; c < CPU_SETSIZE; ++c)
        if (CPU_ISSET(c, &set))
            cpus.push_back(c);
    return cpus;
}

}

TEST(PerCPU, PinnedThreadSeesOwnCPU)
{
    cpu_set_t saved;
    if (sched_getaffinity(0, sizeof(saved), &saved) != 0)
        GTEST_SKIP() << "cannot read affinity";

    size_t tested = 0;
    for (int cpu : allowedCpus())
    {
        if (!pinTo(cpu))
            continue;
        EXPECT_EQ(PerCPU::getCurrentCPU(), cpu);
        /// A few CPUs suffice; keep the test quick on large machines.
        if (++tested >= 4)
            break;
    }

    sched_setaffinity(0, sizeof(saved), &saved);
    if (tested == 0)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// The per-thread setup behind getCurrentCPU is lazy: a fresh thread must report the correct
/// id starting from its very first call.
TEST(PerCPU, FreshThreadSeesOwnCPU)
{
    const auto cpus = allowedCpus();
    if (cpus.empty())
        GTEST_SKIP() << "cannot read affinity";

    size_t tested = 0;
    for (size_t i = 0; i < cpus.size() && tested < 4; ++i)
    {
        const int cpu = cpus[i];
        std::thread([&]
        {
            if (!pinTo(cpu))
                return;
            EXPECT_EQ(PerCPU::getCurrentCPU(), cpu);
            ++tested;
        }).join();
    }
    if (tested == 0)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// An rseq-backed thread always knows its CPU.
TEST(PerCPU, HaveRSeqImpliesKnownCPU)
{
    std::thread([]
    {
        if (PerCPU::haveRSeq())
            EXPECT_GE(PerCPU::getCurrentCPU(), 0);
    }).join();
}

#endif
