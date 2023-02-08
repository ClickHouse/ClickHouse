#include <gtest/gtest.h>

#include <thread>
#include <condition_variable>
#include <shared_mutex>
#include <barrier>
#include <atomic>

#include "Common/Exception.h"
#include <Common/CancelTokenGroup.h>
#include <Common/CancelableSharedMutex.h>

#ifdef OS_LINUX /// These tests require cancellability

namespace DB
{
    namespace ErrorCodes
    {
        extern const int THREAD_WAS_CANCELED;
    }
}

using namespace DB;

struct EnterGroup
{
    CancelTokenGroup & group;

    explicit EnterGroup(CancelTokenGroup & group_)
        : group(group_)
    {
        group.enterGroup();
    }

    ~EnterGroup()
    {
        group.exitGroup();
    }
};

TEST(CancelTokenGroupTest, Simple)
{
    for (int writers = 1; writers <= 128; writers *= 2)
    {
        CancelTokenGroup group;

        CancelableSharedMutex sm;
        std::atomic<int> test = 0;

        std::vector<std::thread> threads;

        threads.reserve(writers);
        auto writer = [&]
        {
            try
            {
                EnterGroup scoped(group);
                while (true)
                {
                    std::shared_lock lock(sm);
                    test++;
                }
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.code(), ErrorCodes::THREAD_WAS_CANCELED);
            }
        };

        for (int i = 0; i < writers; i++)
            threads.emplace_back(writer);

        // Wait a little bit
        while (test.load() < 1000 * writers) {}

        {
            std::unique_lock lock(sm); // Threads should be able to finish even if they were blocked
            group.cancelGroup();

            for (auto & thread : threads)
                thread.join();
        }
    }
}

TEST(CancelTokenGroupTest, RecycleThreads)
{
    struct Job
    {
        CancelTokenGroup group;
        CancelableSharedMutex sm;
        std::atomic<int> test = 0;

        String name() const
        {
            return fmt::format("job:{}", reinterpret_cast<const void*>(this));
        }
    };

    for (int writers = 1; writers <= 128; writers *= 2)
    {
        std::array<Job, 10> jobs;
        std::vector<std::thread> threads;

        threads.reserve(writers);
        auto writer = [&]
        {
            for (auto & job : jobs)
            {
                try
                {
                    EnterGroup scoped(job.group);
                    while (true)
                    {
                        CancelToken::throwIfCanceled(); // cancelation point is required for the last writer which is going never wait (thus, hang up)
                        std::unique_lock lock(job.sm);
                        job.test++;
                    }
                }
                catch (Exception & e)
                {
                    ASSERT_EQ(e.code(), ErrorCodes::THREAD_WAS_CANCELED);
                    ASSERT_EQ(e.message(), job.name());
                }
            }
        };

        for (int i = 0; i < writers; i++)
            threads.emplace_back(writer);

        for (auto & job : jobs)
        {
            while (job.test.load() < 100 * writers) {} // Wait a little bit
            job.group.cancelGroup(ErrorCodes::THREAD_WAS_CANCELED, job.name());
        }

        for (auto & thread : threads)
            thread.join();
    }
}

TEST(CancelTokenGroupTest, EnterCanceledGroup)
{
    for (int writers = 2; writers <= 128; writers *= 2)
    {
        CancelTokenGroup group;

        CancelableSharedMutex sm;
        std::atomic<int> test = 0;

        std::vector<std::thread> threads;

        threads.reserve(writers);
        std::barrier sync(writers / 2 + 1);
        auto writer = [&]
        {
            try
            {
                EnterGroup scoped(group);
                sync.arrive_and_wait(); // (A) sync with leader thread
                while (true)
                {
                    std::shared_lock lock(sm);
                    test++;
                }
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.code(), ErrorCodes::THREAD_WAS_CANCELED);
            }
        };

        for (int i = 0; i < writers / 2; i++)
            threads.emplace_back(writer);

        sync.arrive_and_wait(); // (A) leader thread waits for half of writers to enter the group
        group.cancelGroup(); // cancel before the other half of writers enter the group

        // Start the other half of writers to check if they will also be canceled on enetering the group
        for (int i = 0; i < writers / 2; i++)
            threads.emplace_back(writer);

        sync.arrive_and_wait(); // (A) just sync with the other half of writers

        std::unique_lock lock(sm); // All threads should now throw on attempt to acquire mutex
        for (auto & thread : threads)
            thread.join();
    }
}

#endif
