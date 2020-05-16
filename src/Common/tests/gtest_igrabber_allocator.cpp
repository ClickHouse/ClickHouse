#include <gtest/gtest.h>
#include <Common/IGrabberAllocator.h>

using namespace DB;

using IntToInt = IGrabberAllocator<int, int>;

TEST(IGrabberAllocator, InvalidMaxSize)
{
    /// Default case.
    EXPECT_ANY_THROW(IntToInt(ga::defaultMinChunkSize - 10000));

    using Explt = IGrabberAllocator<int, int, std::hash<int>, ga::runtime, ga::runtime, ga::DefaultASLR, 10000>;

    /// Explicit MinChunkSize specialization
    EXPECT_ANY_THROW(Explt{800});
}

TEST(IGrabberAllocator, SingleInsertionMultipleRetrieval)
{
    IntToInt cache(ga::defaultMinChunkSize);

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

    cache.reset();
    EXPECT_EQ(cache.getStats(), ga::Stats{});
}

