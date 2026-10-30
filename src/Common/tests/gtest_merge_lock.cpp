#include <Common/MergeLock.h>

#include <gtest/gtest.h>

#include <type_traits>
#include <vector>

namespace
{

struct TSA_CAPABILITY("TrackingMutex") TrackingMutex
{
    std::vector<TrackingMutex *> & acquisitions;
    bool shared = false;
    bool exclusive = false;
    bool throw_on_lock = false;

    void lock()
    {
        if (throw_on_lock)
            throw std::runtime_error("Lock acquisition failed");
        acquisitions.push_back(this);
        exclusive = true;
    }

    void lock_shared()
    {
        if (throw_on_lock)
            throw std::runtime_error("Lock acquisition failed");
        acquisitions.push_back(this);
        shared = true;
    }

    void unlock() { exclusive = false; }
    void unlock_shared() { shared = false; }
};

static_assert(!std::is_copy_constructible_v<DB::MergeLock<TrackingMutex>>);
static_assert(!std::is_copy_assignable_v<DB::MergeLock<TrackingMutex>>);
static_assert(!std::is_move_constructible_v<DB::MergeLock<TrackingMutex>>);
static_assert(!std::is_move_assignable_v<DB::MergeLock<TrackingMutex>>);

TEST(MergeLock, ModesAndAddressOrder)
{
    for (bool reverse : {false, true})
    {
        std::vector<TrackingMutex *> acquisitions;
        TrackingMutex mutexes[] = {{acquisitions}, {acquisitions}};
        auto & source = mutexes[reverse ? 1 : 0];
        auto & destination = mutexes[reverse ? 0 : 1];
        {
            DB::MergeLock lock(source, destination);
            EXPECT_TRUE(source.shared);
            EXPECT_FALSE(source.exclusive);
            EXPECT_TRUE(destination.exclusive);
            EXPECT_FALSE(destination.shared);
            ASSERT_EQ(acquisitions.size(), 2);
            EXPECT_EQ(acquisitions[0], &mutexes[0]);
            EXPECT_EQ(acquisitions[1], &mutexes[1]);
        }
        EXPECT_FALSE(source.shared);
        EXPECT_FALSE(destination.exclusive);
    }
}

void acquireMergeLock(TrackingMutex & source, TrackingMutex & destination)
{
    DB::MergeLock lock(source, destination);
}

TEST(MergeLock, RejectsSameMutex)
{
    std::vector<TrackingMutex *> acquisitions;
    TrackingMutex mutex{acquisitions};
    EXPECT_THROW(acquireMergeLock(mutex, mutex), std::invalid_argument);
    EXPECT_TRUE(acquisitions.empty());
    EXPECT_FALSE(mutex.shared);
    EXPECT_FALSE(mutex.exclusive);
}

TEST(MergeLock, ReleasesFirstLockWhenSecondThrows)
{
    for (bool reverse : {false, true})
    {
        std::vector<TrackingMutex *> acquisitions;
        TrackingMutex mutexes[] = {{acquisitions}, {acquisitions}};
        mutexes[1].throw_on_lock = true;
        auto & source = mutexes[reverse ? 1 : 0];
        auto & destination = mutexes[reverse ? 0 : 1];
        EXPECT_THROW((DB::MergeLock(source, destination)), std::runtime_error);
        ASSERT_EQ(acquisitions.size(), 1);
        EXPECT_EQ(acquisitions[0], &mutexes[0]);
        for (const auto & mutex : mutexes)
        {
            EXPECT_FALSE(mutex.shared);
            EXPECT_FALSE(mutex.exclusive);
        }
    }
}

}
