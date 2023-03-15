#include <gtest/gtest.h>

#include <vector>
#include <thread>
#include <fmt/format.h>

#include <Storages/MergeTree/PartitionActionBlocker.h>

using namespace DB;

TEST(PartitionActionBlocker, TestDefaultConstructor)
{
    PartitionActionBlocker blocker;

    EXPECT_FALSE(blocker.isCancelled());
    // EXPECT_EQ(0, blocker.getCounter().load());
}

TEST(PartitionActionBlocker, TestCancelForever)
{
    PartitionActionBlocker blocker;

    blocker.cancelForever();
    EXPECT_TRUE(blocker.isCancelled());
    // EXPECT_EQ(1, blocker.getCounter().load());
}

TEST(PartitionActionBlocker, TestCancel)
{
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    {
        auto lock = blocker.cancel();
        EXPECT_TRUE(blocker.isCancelled());
        // EXPECT_EQ(1, blocker.getCounter().load());
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelled());
}

TEST(PartitionActionBlocker, TestCancelForPartition)
{
    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

    {
        auto lock = blocker.cancelForPartition(partition_id);

        // blocker is not locked fully
        EXPECT_FALSE(blocker.isCancelled());
        // EXPECT_EQ(0, blocker.getCounter().load());

        // blocker reports that only partition is locked
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));

        // doesn't affect other partitions
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
}

TEST(PartitionActionBlocker, TestCancelForTwoPartitions)
{
    const std::string partition_id1 = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id1);
        auto lock2 = blocker.cancelForPartition(partition_id2);

        // blocker is not locked fully
        EXPECT_FALSE(blocker.isCancelled());
        // EXPECT_EQ(0, blocker.getCounter().load());

        // blocker reports that both partitions are locked
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id1));
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
    }

    // blocker is not locked fully
    EXPECT_FALSE(blocker.isCancelled());
    // EXPECT_EQ(0, blocker.getCounter().load());

    // blocker reports that only partition is locked
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id1));
}

TEST(PartitionActionBlocker, TestCancelForSamePartitionTwice)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";

    // Lock `partition_id` twice, make sure that global lock
    // and other partitions are unaffected.
    // Check that `partition_id` is unlocked only after both locks are destroyed.

    PartitionActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id);
        {
            auto lock2 = blocker.cancelForPartition(partition_id);

            EXPECT_FALSE(blocker.isCancelled());
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
            EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
        }

        EXPECT_FALSE(blocker.isCancelled());
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }
    // All locks lifted

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
}

TEST(PartitionActionBlocker, TestCancelAndThenCancelForPartition)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

    {
        auto global_lock = blocker.cancel();

        {
            auto lock1 = blocker.cancelForPartition(partition_id);
            EXPECT_TRUE(blocker.isCancelled());
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
        }

        EXPECT_TRUE(blocker.isCancelled());
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
    }
    // All locks lifted

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
}


TEST(PartitionActionBlocker, TestCancelForPartitionAndThenCancel)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id);
        {
            auto global_lock = blocker.cancel();

            EXPECT_TRUE(blocker.isCancelled());
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
        }

        // global_locked is 'no more', so only 1 partition is locked now.
        EXPECT_FALSE(blocker.isCancelled());
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }
    // All locks lifted

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
}

TEST(PartitionActionBlocker, TestAutomaticCompactPartitionBlockers)
{
    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }

    // Automatic cleanup happens once in a while, 100 stale locks should trigger it.
    EXPECT_LT(blocker.countPartitionBlockers(), partitions_count);
}

TEST(PartitionActionBlocker, TestCompactPartitionBlockers)
{
    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }
    // Manually cleanup all stale blockers (all blockers in this case).
    blocker.compactPartitionBlockers();

    EXPECT_EQ(0, blocker.countPartitionBlockers());
}

TEST(PartitionActionBlocker, TestCompactPartitionBlockersDoesntRemoveActiveBlockers)
{
    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    auto lock_foo = blocker.cancelForPartition("FOO");
    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }
    auto lock_bar = blocker.cancelForPartition("BAR");

    EXPECT_LT(2, blocker.countPartitionBlockers());

    // Manually cleanup all stale blockers (all except held by lock_foo and lock_bar).
    blocker.compactPartitionBlockers();

    EXPECT_EQ(2, blocker.countPartitionBlockers());
}

TEST(PartitionActionBlocker, TestFormatDebug)
{
    // Do not validate contents, just make sure that something is printed out

    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    auto global_lock = blocker.cancel();
    auto lock_foo = blocker.cancelForPartition("FOO");
    auto lock_foo2 = blocker.cancelForPartition("FOO");
    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }
    auto lock_bar = blocker.cancelForPartition("BAR");

    EXPECT_NE("", blocker.formatDebug());
}

namespace std {
template <typename R, typename P>
inline ostream & operator<<(ostream & ostr, const chrono::duration<R, P> & d) {

    const auto seconds = d.count() * 1.0 * P::num / P::den;
    return ostr << std::fixed << std::setprecision(1) << seconds << "s";
}
}

template <>
struct fmt::formatter<std::thread::id> : public fmt::formatter<std::string>
{
    auto format(const std::thread::id & id, fmt::format_context& ctx) const
    {
        std::stringstream sstr;
        sstr << id;

        return fmt::formatter<std::string>::format(sstr.str(), ctx);
    }
};

template <typename R, typename P>
struct fmt::formatter<std::chrono::duration<R, P>> : public fmt::formatter<std::string>
{
    auto format(const std::chrono::duration<R, P> & duration, fmt::format_context& ctx) const
    {
        std::stringstream sstr;
        sstr << duration;

        return fmt::formatter<std::string>::format(sstr.str(), ctx);
    }
};

// TODO: remove this before merging
TEST(PartitionActionBlocker, DISABLED_TestMultiThreadLocks)
{
    // Do not validate contents, just make sure that nothing crashes.

    const size_t WORKERS_COUNT = 100;
    const size_t DEFAULT_ITERATIONS = 100'000;
    const auto DEFAULT_DELAY = std::chrono::microseconds{100};

    std::vector<std::thread> workers;
    PartitionActionBlocker blocker;
//#define LOG_PRINT(...) (void)(0)
#define LOG_PRINT fmt::print

#define LOG_JOB_START(JOB_NAME) LOG_PRINT(stderr, "!!! Starting a job {} that will take at least {} on thread {}\n", \
    JOB_NAME, \
    repetitions * delay, std::this_thread::get_id())

    auto partition_locker = [&blocker](size_t worker_id, size_t repetitions, std::chrono::microseconds delay)
    {
        LOG_JOB_START("partition_locker");

        const std::string partition_id = "partition_" + std::to_string(worker_id);
        for (size_t i = 0; i < repetitions; ++i)
        {
            auto lock = blocker.cancelForPartition(partition_id);
            if (i != repetitions-1)
                std::this_thread::sleep_for(delay);
        }
    };

    auto global_locker = [&blocker](size_t repetitions, std::chrono::microseconds delay)
    {
        LOG_JOB_START("global_locker");

        for (size_t i = 0; i < repetitions; ++i)
        {
            auto lock = blocker.cancel();
            if (i != repetitions-1)
                std::this_thread::sleep_for(delay);
        }
    };

    auto try_execute_on_partition = [&blocker](size_t worker_id, auto action, size_t repetitions, std::chrono::microseconds delay)
    {
        LOG_JOB_START("try_execute_on_partition");

        const std::string partition_id = "partition_" + std::to_string(worker_id);
        for (size_t i = 0; i < repetitions; ++i)
        {
            if (!blocker.isCancelledForPartition(partition_id))
            {
                action(partition_id);
            }
            if (i != repetitions-1)
                std::this_thread::sleep_for(delay);
        }
    };

    std::atomic<size_t> successfull_partition_actions = 0;

    auto print_partition_id = [&successfull_partition_actions](const std::string & /*partition_id*/)
    {
        ++successfull_partition_actions;
        //LOG_PRINT(stderr, "Got UNLOCKED partition {} on {}\n", partition_id, std::this_thread::get_id());
    };

    auto print_debug = [&blocker](size_t repetitions, std::chrono::microseconds delay, const char * prefix = "")
    {
        if (repetitions > 1)
            LOG_JOB_START("print_debug");

        for (size_t i = 0; i < repetitions; ++i)
        {
            LOG_PRINT(stderr, "!!! {} Debug {} \n {}\n", prefix, i, blocker.formatDebug());

            if (i != repetitions-1)
                std::this_thread::sleep_for(delay);
        }
    };

    for (size_t w = 0; w < WORKERS_COUNT; ++w)
    {
        workers.emplace_back(partition_locker, w % 100, DEFAULT_ITERATIONS, DEFAULT_DELAY);
    }

    for (size_t w = 0; w < WORKERS_COUNT * 2; ++w)
    {
        workers.emplace_back(try_execute_on_partition, w % 100, print_partition_id, DEFAULT_ITERATIONS, DEFAULT_DELAY);
    }

    for (size_t w = 0; w < 10; ++w)
    {
        workers.emplace_back(global_locker, DEFAULT_ITERATIONS / 10, DEFAULT_DELAY * 10);
    }

    workers.emplace_back(print_debug, 5, std::chrono::seconds(1), "SCHEDULED");

    std::this_thread::sleep_for(std::chrono::seconds(6));

    LOG_PRINT(stderr, "!!! Joining {} threads\n", workers.size());

    size_t t = 0;
    for (auto & w : workers)
    {
        LOG_PRINT(stderr, "\tJoining thread #{} {} ...", t++, w.get_id());
        w.join();
        LOG_PRINT(stderr, "\tjoinied OK\n");

    }
    LOG_PRINT(stderr, "!!! Joined all {} threads", workers.size());
    std::cerr << successfull_partition_actions << " times seen unlocked partition" << std::endl;

    print_debug(1, std::chrono::microseconds{0}, "POST-JOIN");
}
