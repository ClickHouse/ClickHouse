#include <gtest/gtest.h>
#include <Common/Allocators/IGrabberAllocator.h>

using namespace DB;

/**
 * Let's call the cache @e stateless if the test does not check the mmaped memory,
 * and @e stateful otherwise.
 */

using IntToInt = IGrabberAllocator<int, int>;

struct pointer { int * ptr; };
using IntToPointer = IGrabberAllocator<int, pointer>;

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
    EXPECT_ANY_THROW(IntToInt(MMAP_THRESHOLD - 10));

    using Explt = IGrabberAllocator<int, int,
          std::hash<int>,
          ga::Runtime,
          ga::Runtime,
          AllocatorsASLR, 10000>;

    /// Explicit MinChunkSize specialization
    EXPECT_ANY_THROW(Explt{800});
}

TEST(IGrabberAllocator, StatelessSingleInsertionSingleRetrieval)
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

        /// (1) [start + size; start + page_size] originally allocated region (size 4096).
        /// (2) [start; start + size] region chopped from first's head (size 200).


        EXPECT_EQ(stats.misses, 2); //get + getOrSet
        EXPECT_EQ(stats.hits, 0);

        EXPECT_EQ(stats.used_regions, 1); /// (1)
        EXPECT_EQ(stats.free_regions, 1); /// (2)
        EXPECT_EQ(stats.regions, 2);

        auto ptr2 = cache.get(0);

        stats = cache.getStats();

        EXPECT_EQ(ptr.get(), ptr2.get());

        EXPECT_EQ(stats.misses, 2);
        EXPECT_EQ(stats.hits, 1);

        /// assert that no more regions were created.
        EXPECT_EQ(stats.used_regions, 1); /// (1)
        EXPECT_EQ(stats.free_regions, 1); /// (2)
        EXPECT_EQ(stats.regions, 2);
    }

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.unused_regions, 1); /// (1)
    EXPECT_EQ(stats.free_regions, 1); /// (2)
    EXPECT_EQ(stats.regions, 2);
}

pointer init(void * p)
{
    int * i = static_cast<int*>(p);
    *i = 42;
    return pointer{i};
}

TEST(IGrabberAllocator, StatefulSingleInsertionSingleRetrieval)
{
    IntToPointer cache(MMAP_THRESHOLD);

    EXPECT_EQ(cache.getStats(), ga::Stats{});

    EXPECT_EQ(cache.get(0), std::shared_ptr<pointer>{nullptr});

    ga::Stats stats;

    {
        auto&& [ptr, produced] = cache.getOrSet(0, []{ return 200; }, init);

        /// (1) [start + size; start + page_size] originally allocated region (size 4096).
        /// (2) [start; start + size] region chopped from first's head (size 200).

        EXPECT_TRUE(produced);
        EXPECT_EQ(*(ptr->ptr), 42);

        stats = cache.getStats();

        EXPECT_EQ(stats.misses, 2); //get + getOrSet
        EXPECT_EQ(stats.hits, 0);

        EXPECT_EQ(stats.used_regions, 1);
        EXPECT_EQ(stats.free_regions, 1);
        EXPECT_EQ(stats.regions, 2);

        auto ptr2 = cache.get(0);

        stats = cache.getStats();

        EXPECT_EQ(*(ptr2->ptr), 42);
        EXPECT_EQ(ptr.get(), ptr2.get());

        EXPECT_EQ(stats.misses, 2);
        EXPECT_EQ(stats.hits, 1);

        EXPECT_EQ(stats.used_regions, 1);
        EXPECT_EQ(stats.free_regions, 1);
        EXPECT_EQ(stats.regions, 2);
    }

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.unused_regions, 1); /// (1)
    EXPECT_EQ(stats.free_regions, 1); /// (2)
    EXPECT_EQ(stats.regions, 2);
}

TEST(IGrabberAllocator, StatelessCacheUnusedShrinking)
{
    IntToInt cache(MMAP_THRESHOLD);

    const auto size = [] { return 100; };
    const auto init = [](void *) { return 0; };

    ga::Stats stats;

    {
        /// (1) [start + 100; start + page_size] originally allocated region.
        /// (2) [start; start + 100] region chopped from first's head (size 100).

        auto pair = cache.getOrSet(0, size, init);
        stats = cache.getStats();

        EXPECT_EQ(stats.regions, 2);
        EXPECT_EQ(stats.used_regions, 1); /// (2)
        EXPECT_EQ(stats.free_regions, 1); /// (1)
        EXPECT_EQ(stats.unused_regions, 0);
    }

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.free_regions, 1);   /// (2)
    EXPECT_EQ(stats.unused_regions, 1); /// (1)

    {
        /// (1) [start + 200; start + page_size] originally allocated region.
        /// (2) [start; start + 100] region chopped from first's head (size 100).
        /// (3) [start + 100; start + 200] second allocated region (from first).

        auto pair = cache.getOrSet(1, size, init);

        stats = cache.getStats();

        EXPECT_EQ(stats.regions, 3);
        EXPECT_EQ(stats.used_regions, 1);   /// (3)
        EXPECT_EQ(stats.unused_regions, 1); /// (2)
        EXPECT_EQ(stats.free_regions, 1);   /// (1)
    }

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.unused_regions, 2); /// (2), (3)
    EXPECT_EQ(stats.free_regions, 1);   /// (1)

    EXPECT_EQ(stats.chunks, 1);

    cache.shrinkToFit();

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.unused_regions, 0);
    EXPECT_EQ(stats.free_regions, 0);
    EXPECT_EQ(stats.regions, 0);

    EXPECT_EQ(stats.chunks, 0);

    EXPECT_EQ(cache.get(0).get(), nullptr);
    EXPECT_EQ(cache.get(1).get(), nullptr);
}

TEST(IGrabberAllocator, StatefulCacheUnusedShrinking)
{
    IntToPointer cache(MMAP_THRESHOLD);

    const auto size = [] { return sizeof(pointer); };

    {
        auto&& [ptr, _] = cache.getOrSet(0, size, init);
        auto stats = cache.getStats();

        EXPECT_EQ(*(ptr->ptr), 42);

        EXPECT_EQ(stats.regions, 2);
        EXPECT_EQ(stats.used_regions, 1);
        EXPECT_EQ(stats.unused_regions, 0);
    }

    {
        auto&& [ptr, _] = cache.getOrSet(1, size, init);
        auto stats = cache.getStats();

        EXPECT_EQ(*(ptr->ptr), 42);

        EXPECT_EQ(stats.regions, 3);
        EXPECT_EQ(stats.used_regions, 1);
        EXPECT_EQ(stats.unused_regions, 1);
    }

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.unused_regions, 2); /// (2), (3)
    EXPECT_EQ(stats.free_regions, 1);   /// (1)

    EXPECT_EQ(stats.initialized_size, 2 * sizeof(pointer));
    EXPECT_EQ(stats.chunks, 1);

    cache.shrinkToFit();

    stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 0);
    EXPECT_EQ(stats.unused_regions, 0);
    EXPECT_EQ(stats.free_regions, 0);
    EXPECT_EQ(stats.regions, 0);

    EXPECT_EQ(stats.chunks, 0);

    EXPECT_EQ(cache.get(0).get(), nullptr);
    EXPECT_EQ(cache.get(1).get(), nullptr);
}

TEST(IGrabberAllocator, StatelessCacheUsedShrinking)
{
    IntToInt cache(MMAP_THRESHOLD);

    const auto size = [] {return 100; };
    const auto init = [](void *) {return 100; };

    auto&& [ptr, produced] = cache.getOrSet(0, size, init);

    {
        cache.getOrSet(1, size, init);
    }

    ga::Stats stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 1);
    EXPECT_EQ(stats.free_regions, 1);
    EXPECT_EQ(stats.unused_regions, 1);

    cache.shrinkToFit();

    stats = cache.getStats();

    EXPECT_EQ(stats.chunks, 1);

    EXPECT_EQ(stats.used_regions, 1);
    EXPECT_EQ(stats.unused_regions, 0);
    EXPECT_EQ(stats.free_regions, 0);
    EXPECT_EQ(stats.regions, 1);

    EXPECT_EQ(cache.get(1).get(), nullptr);
    EXPECT_EQ(cache.get(0).get(), ptr.get());
}

TEST(IGrabberAllocator, StatefulCacheUsedShrinking)
{
    IntToPointer cache(MMAP_THRESHOLD);

    const auto size = [] {return sizeof(pointer); };

    auto&& [ptr, produced] = cache.getOrSet(0, size, init);

    EXPECT_EQ(*(ptr->ptr), 42);

    {
        cache.getOrSet(1, size, init);
        EXPECT_EQ(*(ptr->ptr), 42);
    }

    auto stats = cache.getStats();

    EXPECT_EQ(stats.used_regions, 1);
    EXPECT_EQ(stats.unused_regions, 1);

    cache.shrinkToFit();

    stats = cache.getStats();

    EXPECT_EQ(stats.chunks, 1);

    EXPECT_EQ(stats.used_regions, 1);
    EXPECT_EQ(stats.unused_regions, 0);
    EXPECT_EQ(stats.free_regions, 0);
    EXPECT_EQ(stats.regions, 1);

    EXPECT_EQ(cache.get(1).get(), nullptr);
    EXPECT_EQ(cache.get(0).get(), ptr.get());
    EXPECT_EQ(*(ptr->ptr), 42);
}

