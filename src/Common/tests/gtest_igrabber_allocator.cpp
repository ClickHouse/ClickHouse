#include <gtest/gtest.h>
#include <Common/Allocators/IGrabberAllocator.h>

using namespace DB;

using IntToInt = IGrabberAllocator<int, int>;

namespace ga
{
bool operator == (const Stats &one, const Stats& other) noexcept
{
    return !memcmp(&one, &other, sizeof(Stats));
}
}

TEST(IGrabberAllocator, InvalidMaxSize)
{
    /// Default case.
    EXPECT_ANY_THROW(IntToInt(MMAP_THRESHOLD - 10000));

    using Explt = IGrabberAllocator<int, int,
          std::hash<int>,
          ga::Runtime,
          ga::Runtime,
          AllocatorsASLR, 10000>;

    /// Explicit MinChunkSize specialization
    EXPECT_ANY_THROW(Explt{800});
}

TEST(IGrabberAllocator, SingleInsertionSingleRetrieval)
{
    IntToInt cache(MMAP_THRESHOLD);

    EXPECT_EQ(cache.getStats(), ga::Stats{});

    EXPECT_EQ(cache.get(0), std::shared_ptr<int>{nullptr});

    ga::Stats stats;

    {
        auto&& [ptr, produced] = cache.getOrSet(0,
                []{ return 200; },
                [](void *) {return 100;});

        EXPECT_TRUE(produced);

        stats = cache.getStats();

        EXPECT_EQ(stats.misses, 1);
        EXPECT_EQ(stats.used_regions_count, 1);
        EXPECT_EQ(stats.all_regions_count, 1);

        auto ptr2 = cache.get(0);

        stats = cache.getStats();

        EXPECT_EQ(ptr.get(), ptr2.get());
        EXPECT_EQ(stats.misses, 1);
        EXPECT_EQ(stats.hits, 1);
        EXPECT_EQ(stats.used_regions_count, 1);
        EXPECT_EQ(stats.all_regions_count, 1);
    }

    stats = cache.getStats();

    EXPECT_EQ(stats.keyed_regions_count, 0);
    EXPECT_EQ(stats.all_regions_count, 1);
}

