#include <Storages/MergeTree/MergeList.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace DB
{
namespace
{

TEST(MergeList, TTLMergeCapacityPredicate)
{
    EXPECT_FALSE(canReserveMergeWithTTL(0, 0, MergeType::TTLDelete));
    EXPECT_FALSE(canReserveMergeWithTTL(0, 0, MergeType::TTLClearIndex));
    EXPECT_TRUE(canReserveMergeWithTTL(0, 1, MergeType::TTLDelete));
    EXPECT_FALSE(canReserveMergeWithTTL(0, 1, MergeType::TTLClearIndex));

    EXPECT_TRUE(canReserveMergeWithTTL(0, 2, MergeType::TTLClearIndex));
    EXPECT_FALSE(canReserveMergeWithTTL(1, 2, MergeType::TTLClearIndex));
    EXPECT_TRUE(canReserveMergeWithTTL(1, 2, MergeType::TTLDelete));
    EXPECT_FALSE(canReserveMergeWithTTL(2, 2, MergeType::TTLDelete));

    EXPECT_TRUE(canReserveMergeWithTTL(2, 4, MergeType::TTLClearIndex));
    EXPECT_FALSE(canReserveMergeWithTTL(3, 4, MergeType::TTLClearIndex));
    EXPECT_TRUE(canReserveMergeWithTTL(3, 4, MergeType::TTLRecompress));
}

TEST(MergeList, TTLClearIndexReservationLeavesCapacityForDelete)
{
    MergeList merge_list;
    auto clear_index = merge_list.tryReserveMergeWithTTL(MergeType::TTLClearIndex, 2);
    ASSERT_TRUE(clear_index);
    EXPECT_FALSE(merge_list.tryReserveMergeWithTTL(MergeType::TTLClearIndex, 2));
    auto ttl_delete = merge_list.tryReserveMergeWithTTL(MergeType::TTLDelete, 2);
    ASSERT_TRUE(ttl_delete);
    EXPECT_EQ(merge_list.getMergesWithTTLCount(), 2);
    clear_index.reset();
    ttl_delete.reset();
}

TEST(MergeList, ConcurrentTTLClearIndexReservationsLeaveCapacity)
{
    MergeList merge_list;
    std::atomic<size_t> reservations = 0;
    std::vector<std::optional<TTLMergeReservation>> held_reservations(32);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < 32; ++i)
    {
        threads.emplace_back([&, i]
        {
            held_reservations[i] = merge_list.tryReserveMergeWithTTL(MergeType::TTLClearIndex, 2);
            if (held_reservations[i])
                ++reservations;
        });
    }
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(reservations, 1);
    EXPECT_EQ(merge_list.getMergesWithTTLCount(), 1);
    held_reservations.clear();
    EXPECT_EQ(merge_list.getMergesWithTTLCount(), 0);
}

TEST(MergeList, TTLMergeReservationReleasesOnDestruction)
{
    MergeList merge_list;
    {
        auto reservation = merge_list.tryReserveMergeWithTTL(MergeType::TTLClearIndex, 4);
        ASSERT_TRUE(reservation);
        EXPECT_EQ(merge_list.getMergesWithTTLCount(), 1);
    }
    EXPECT_EQ(merge_list.getMergesWithTTLCount(), 0);
}

TEST(MergeList, TTLMergeReservationCASLimits)
{
    for (size_t maximum : {0, 1, 2, 8})
    {
        MergeList merge_list;
        std::vector<TTLMergeReservation> reservations;
        while (auto reservation = merge_list.tryReserveMergeWithTTL(MergeType::TTLClearIndex, maximum))
            reservations.emplace_back(std::move(*reservation));
        EXPECT_EQ(reservations.size(), maximum == 0 ? 0 : maximum - 1);
        EXPECT_EQ(merge_list.getMergesWithTTLCount(), reservations.size());
    }
}

}
}
