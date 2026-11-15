#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <base/types.h>
#include <base/phdr_cache.h>
#include <random>
#include <pcg_random.hpp>
#include <thread>
#include <atomic>
#include <iomanip>


using namespace DB;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int DEADLOCK_AVOIDED;
    }
}


namespace
{
    class Events
    {
    public:
        Events() : start_time(std::chrono::steady_clock::now()) {}

        void add(String && event, std::chrono::milliseconds correction = std::chrono::milliseconds::zero())
        {
            String timepoint = std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count());
            if (timepoint.length() < 5)
                timepoint.insert(0, 5 - timepoint.length(), ' ');
            if (correction.count())
                std::this_thread::sleep_for(correction);
            std::lock_guard lock{mutex};
            //std::cout << timepoint << " : " << event << std::endl;
            events.emplace_back(std::move(event));
        }

        void check(const Strings & expected_events)
        {
            std::lock_guard lock{mutex};
            EXPECT_EQ(events.size(), expected_events.size());
            for (size_t i = 0; i != events.size(); ++i)
                EXPECT_EQ(events[i], (i < expected_events.size() ? expected_events[i] : ""));
        }

    private:
        const std::chrono::time_point<std::chrono::steady_clock> start_time;
        Strings events TSA_GUARDED_BY(mutex);
        mutable std::mutex mutex;
    };
}


TEST(Common, RWLock1)
{
    /// Tests with threads require this, because otherwise
    ///  when tested under Memory Sanitizer,
    ///  it tries to obtain stack trace on 'free' invocation at thread exit,
    ///  but cannot do that due to infinite recursion.
    /// Alternative solution: disable PHDR Cache under memory sanitizer.
    updatePHDRCache();

    constexpr int cycles = 1000;
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static std::atomic<int> readers{0};
    static std::atomic<int> writers{0};

    static auto fifo_lock = RWLockImpl::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    auto func = [&] (size_t threads, int round)
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto type = (std::uniform_int_distribution<>(0, 9)(gen) >= round) ? RWLockImpl::Read : RWLockImpl::Write;
            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));

            auto lock = fifo_lock->getLock(type, RWLockImpl::NO_QUERY);

            if (type == RWLockImpl::Write)
            {
                ++writers;

                ASSERT_EQ(writers, 1);
                ASSERT_EQ(readers, 0);

                std::this_thread::sleep_for(sleep_for);

                --writers;
            }
            else
            {
                ++readers;

                ASSERT_EQ(writers, 0);
                ASSERT_GE(readers, 1);
                ASSERT_LE(readers, threads);

                std::this_thread::sleep_for(sleep_for);

                --readers;
            }
        }
    };

    for (auto pool_size : pool_sizes)
    {
        for (int round = 0; round < 10; ++round)
        {
            Stopwatch watch(CLOCK_MONOTONIC_COARSE);

            std::list<std::thread> threads;
            for (size_t thread = 0; thread < pool_size; ++thread)
                threads.emplace_back([=] () { func(pool_size, round); });

            for (auto & thread : threads)
                thread.join();

            auto total_time = watch.elapsedSeconds();
            std::cout << "Threads " << pool_size << ", round " << round << ", total_time " << std::setprecision(2) << total_time << "\n";
        }
    }
}

TEST(Common, RWLockRecursive)
{
    updatePHDRCache();

    constexpr auto cycles = 10000;

    static auto fifo_lock = RWLockImpl::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    std::thread t1([&] ()
    {
        for (int i = 0; i < 2 * cycles; ++i)
        {
            auto lock = fifo_lock->getLock(RWLockImpl::Write, "q1");

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);
        }
    });

    std::thread t2([&] ()
    {
        for (int i = 0; i < cycles; ++i)
        {
            auto lock1 = fifo_lock->getLock(RWLockImpl::Read, "q2");

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);

            auto lock2 = fifo_lock->getLock(RWLockImpl::Read, "q2");

#ifndef DEBUG_OR_SANITIZER_BUILD
            /// It throws LOGICAL_ERROR
            EXPECT_ANY_THROW({fifo_lock->getLock(RWLockImpl::Write, "q2");});
#endif
        }

        fifo_lock->getLock(RWLockImpl::Write, "q2");
    });

    t1.join();
    t2.join();
}


TEST(Common, RWLockDeadlock)
{
    updatePHDRCache();

    static auto lock1 = RWLockImpl::create();
    static auto lock2 = RWLockImpl::create();

    /**
      * q1: r1          r2
      * q2:    w1
      * q3:       r2       r1
      * q4:          w2
      */

    std::thread t1([&] ()
    {
        auto holder1 = lock1->getLock(RWLockImpl::Read, "q1");
        usleep(100000);
        usleep(100000);
        usleep(100000);
        usleep(100000);
        try
        {
            auto holder2 = lock2->getLock(RWLockImpl::Read, "q1", std::chrono::milliseconds(100));
            if (!holder2)
            {
                throw Exception(ErrorCodes::DEADLOCK_AVOIDED,
                        "Locking attempt timed out! Possible deadlock avoided. Client should retry.");
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DEADLOCK_AVOIDED)
                throw;
        }
    });

    std::thread t2([&] ()
    {
        usleep(100000);
        auto holder1 = lock1->getLock(RWLockImpl::Write, "q2");
    });

    std::thread t3([&] ()
    {
        usleep(100000);
        usleep(100000);
        auto holder2 = lock2->getLock(RWLockImpl::Read, "q3");
        usleep(100000);
        usleep(100000);
        usleep(100000);
        try
        {
            auto holder1 = lock1->getLock(RWLockImpl::Read, "q3", std::chrono::milliseconds(100));
            if (!holder1)
            {
                throw Exception(ErrorCodes::DEADLOCK_AVOIDED,
                        "Locking attempt timed out! Possible deadlock avoided. Client should retry.");
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DEADLOCK_AVOIDED)
                throw;
        }
    });

    std::thread t4([&] ()
    {
        usleep(100000);
        usleep(100000);
        usleep(100000);
        auto holder2 = lock2->getLock(RWLockImpl::Write, "q4");
    });

    t1.join();
    t2.join();
    t3.join();
    t4.join();
}


TEST(Common, RWLockPerfTestReaders)
{
    updatePHDRCache();

    constexpr int cycles = 100000; // 100k
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static auto fifo_lock = RWLockImpl::create();

    for (auto pool_size : pool_sizes)
    {
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);

        auto func = [&] ()
        {
            for (auto i = 0; i < cycles; ++i)
            {
                auto lock = fifo_lock->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY);
            }
        };

        std::list<std::thread> threads;
        for (size_t thread = 0; thread < pool_size; ++thread)
            threads.emplace_back(func);

        for (auto & thread : threads)
            thread.join();

        auto total_time = watch.elapsedSeconds();
        std::cout << "Threads " << pool_size << ", total_time " << std::setprecision(2) << total_time << "\n";
    }
}

TEST(Common, RWLockNotUpgradeableWithNoQuery)
{
    updatePHDRCache();

    static auto rw_lock = RWLockImpl::create();

    std::thread read_thread([&] ()
    {
        auto lock = rw_lock->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY, std::chrono::duration<int, std::milli>(50000));
        auto sleep_for = std::chrono::duration<int, std::milli>(5000);
        std::this_thread::sleep_for(sleep_for);
    });

    {
        auto sleep_for = std::chrono::duration<int, std::milli>(500);
        std::this_thread::sleep_for(sleep_for);

        Stopwatch watch(CLOCK_MONOTONIC_COARSE);
        auto get_lock = rw_lock->getLock(RWLockImpl::Write, RWLockImpl::NO_QUERY, std::chrono::duration<int, std::milli>(50000));

        EXPECT_NE(get_lock.get(), nullptr);
        /// It took some time
        EXPECT_GT(watch.elapsedMilliseconds(), 3000);
    }

    read_thread.join();
}


TEST(Common, RWLockWriteLockTimeoutDuringRead)
{
    /// 0                 100                         200                      300                 400
    /// <---------------------------------------- ra ---------------------------------------------->
    ///                     <----- wc (acquiring lock, failed by timeout) ----->
    ///                                                                                             <wd>
    ///
    ///    0 : Locking ra
    ///    0 : Locked ra
    ///  100 : Locking wc
    ///  300 : Failed to lock wc
    ///  400 : Unlocking ra
    ///  400 : Unlocked ra
    ///  400 : Locking wd
    ///  400 : Locked wd
    ///  400 : Unlocking wd
    ///  400 : Unlocked wd

    static auto rw_lock = RWLockImpl::create();
    Events events;

    std::thread ra_thread([&] ()
    {
        events.add("Locking ra");
        auto ra = rw_lock->getLock(RWLockImpl::Read, "ra");
        events.add(ra ? "Locked ra" : "Failed to lock ra");
        EXPECT_NE(ra, nullptr);

        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(400));

        events.add("Unlocking ra");
        ra.reset();
        events.add("Unlocked ra");
    });

    std::thread wc_thread([&] ()
    {
        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(100));
        events.add("Locking wc");
        auto wc = rw_lock->getLock(RWLockImpl::Write, "wc", std::chrono::milliseconds(200));
        events.add(wc ? "Locked wc" : "Failed to lock wc");
        EXPECT_EQ(wc, nullptr);
    });

    ra_thread.join();
    wc_thread.join();

    {
        events.add("Locking wd");
        auto wd = rw_lock->getLock(RWLockImpl::Write, "wd", std::chrono::milliseconds(1000));
        events.add(wd ? "Locked wd" : "Failed to lock wd");
        EXPECT_NE(wd, nullptr);
        events.add("Unlocking wd");
        wd.reset();
        events.add("Unlocked wd");
    }

    events.check(
        {"Locking ra",
         "Locked ra",
         "Locking wc",
         "Failed to lock wc",
         "Unlocking ra",
         "Unlocked ra",
         "Locking wd",
         "Locked wd",
         "Unlocking wd",
         "Unlocked wd"});
}


TEST(Common, RWLockWriteLockTimeoutDuringTwoReads)
{
    /// 0                 100                         200                         300               400                500
    /// <---------------------------------------- ra ----------------------------------------------->
    ///                     <------ wc (acquiring lock, failed by timeout) ------->
    ///                                                 <-- rb (acquiring lock) --><---------- rb (locked) ------------>
    ///                                                                                                                 <wd>
    ///
    ///    0 : Locking ra
    ///    0 : Locked ra
    ///  100 : Locking wc
    ///  200 : Locking rb
    ///  300 : Failed to lock wc
    ///  300 : Locked rb
    ///  400 : Unlocking ra
    ///  400 : Unlocked ra
    ///  500 : Unlocking rb
    ///  500 : Unlocked rb
    ///  501 : Locking wd
    ///  501 : Locked wd
    ///  501 : Unlocking wd
    ///  501 : Unlocked wd

    static auto rw_lock = RWLockImpl::create();
    Events events;

    std::thread ra_thread([&] ()
    {
        events.add("Locking ra");
        auto ra = rw_lock->getLock(RWLockImpl::Read, "ra");
        events.add(ra ? "Locked ra" : "Failed to lock ra");
        EXPECT_NE(ra, nullptr);

        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(400));

        events.add("Unlocking ra");
        ra.reset();
        events.add("Unlocked ra");
    });

    std::thread rb_thread([&] ()
    {
        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(200));
        events.add("Locking rb");

        auto rb = rw_lock->getLock(RWLockImpl::Read, "rb");

        /// `correction` is used here to add an event to `events` a little later.
        /// (Because the event "Locked rb" happens at nearly the same time as "Failed to lock wc" and we don't want our test to be flaky.)
        auto correction = std::chrono::duration<int, std::milli>(50);
        events.add(rb ? "Locked rb" : "Failed to lock rb", correction);
        EXPECT_NE(rb, nullptr);

        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(200) - correction);
        events.add("Unlocking rb");
        rb.reset();
        events.add("Unlocked rb");
    });

    std::thread wc_thread([&] ()
    {
        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(100));
        events.add("Locking wc");
        auto wc = rw_lock->getLock(RWLockImpl::Write, "wc", std::chrono::milliseconds(200));
        events.add(wc ? "Locked wc" : "Failed to lock wc");
        EXPECT_EQ(wc, nullptr);
    });

    ra_thread.join();
    rb_thread.join();
    wc_thread.join();

    {
        events.add("Locking wd");
        auto wd = rw_lock->getLock(RWLockImpl::Write, "wd", std::chrono::milliseconds(1000));
        events.add(wd ? "Locked wd" : "Failed to lock wd");
        EXPECT_NE(wd, nullptr);
        events.add("Unlocking wd");
        wd.reset();
        events.add("Unlocked wd");
    }

    events.check(
        {"Locking ra",
         "Locked ra",
         "Locking wc",
         "Locking rb",
         "Failed to lock wc",
         "Locked rb",
         "Unlocking ra",
         "Unlocked ra",
         "Unlocking rb",
         "Unlocked rb",
         "Locking wd",
         "Locked wd",
         "Unlocking wd",
         "Unlocked wd"});
}


TEST(Common, RWLockWriteLockTimeoutDuringWriteWithWaitingRead)
{
    /// 0                 100                         200                        300                 400                500
    /// <--------------------------------------------------- wa -------------------------------------------------------->
    ///                     <------ wb (acquiring lock, failed by timeout) ------>
    ///                                                 <-- rc (acquiring lock, failed by timeout) -->
    ///                                                                                                                  <wd>
    ///
    ///    0 : Locking wa
    ///    0 : Locked wa
    ///  100 : Locking wb
    ///  200 : Locking rc
    ///  300 : Failed to lock wb
    ///  400 : Failed to lock rc
    ///  500 : Unlocking wa
    ///  500 : Unlocked wa
    ///  501 : Locking wd
    ///  501 : Locked wd
    ///  501 : Unlocking wd
    ///  501 : Unlocked wd

    static auto rw_lock = RWLockImpl::create();
    Events events;

    std::thread wa_thread([&] ()
    {
        events.add("Locking wa");
        auto wa = rw_lock->getLock(RWLockImpl::Write, "wa");
        events.add(wa ? "Locked wa" : "Failed to lock wa");
        EXPECT_NE(wa, nullptr);

        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(500));

        events.add("Unlocking wa");
        wa.reset();
        events.add("Unlocked wa");
    });

    std::thread wb_thread([&] ()
    {
        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(100));
        events.add("Locking wb");
        auto wc = rw_lock->getLock(RWLockImpl::Write, "wc", std::chrono::milliseconds(200));
        events.add(wc ? "Locked wb" : "Failed to lock wb");
        EXPECT_EQ(wc, nullptr);
    });

    std::thread rc_thread([&] ()
    {
        std::this_thread::sleep_for(std::chrono::duration<int, std::milli>(200));
        events.add("Locking rc");
        auto rc = rw_lock->getLock(RWLockImpl::Read, "rc", std::chrono::milliseconds(200));
        events.add(rc ? "Locked rc" : "Failed to lock rc");
        EXPECT_EQ(rc, nullptr);
    });

    wa_thread.join();
    wb_thread.join();
    rc_thread.join();

    {
        events.add("Locking wd");
        auto wd = rw_lock->getLock(RWLockImpl::Write, "wd", std::chrono::milliseconds(1000));
        events.add(wd ? "Locked wd" : "Failed to lock wd");
        EXPECT_NE(wd, nullptr);
        events.add("Unlocking wd");
        wd.reset();
        events.add("Unlocked wd");
    }

    events.check(
        {"Locking wa",
         "Locked wa",
         "Locking wb",
         "Locking rc",
         "Failed to lock wb",
         "Failed to lock rc",
         "Unlocking wa",
         "Unlocked wa",
         "Locking wd",
         "Locked wd",
         "Unlocking wd",
         "Unlocked wd"});
}
