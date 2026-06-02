#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
    /// Extract the value type T from RoaringBitmapWithSmallSet<T, N>.
    template <typename> struct BitmapValueType;
    template <typename T, UInt8 N>
    struct BitmapValueType<RoaringBitmapWithSmallSet<T, N>> { using type = T; };

    /// Same template instantiation that AggregateFunctionGroupBitmapData<UInt32>/UInt64 uses.
    using BitmapWrapper32 = RoaringBitmapWithSmallSet<UInt32, 32>;
    using BitmapWrapper64 = RoaringBitmapWithSmallSet<UInt64, 32>;

    /// Fill the wrapper with many values spread across many containers so the
    /// underlying CRoaring bitmap has lots of bytes to mutate during a COW copy.
    /// "Containers" here are the 64K chunks inside Roaring; we deliberately
    /// touch ~500 of them so a racy memcpy of containers[]/typecodes[] is wide
    /// enough for TSAN to catch.
    template <typename BitmapType>
    void fill_wide_and_dense(BitmapType & bm, size_t num_containers, size_t values_per_container)
    {
        using ValueType = typename BitmapValueType<BitmapType>::type;
        for (ValueType c = 0; c < num_containers; ++c)
        {
            const ValueType high = c << 16;
            for (ValueType v = 0; v < values_per_container; ++v)
                bm.add(high | v);
        }
    }
}

/// Build one "source" bitmap, then fire N threads that all run
/// `dst[i].merge(*source)`. Each thread's dst starts fresh-empty, so merge()
/// runs `toLarge()` then `*dst.roaring_bitmap |= *source.roaring_bitmap`.
/// With dst empty, that |= short-circuits to roaring_bitmap_overwrite, which
/// under COW (set by the PR in toLarge/read) writes the source's
/// containers[i] pointer and typecodes[i] byte - wrapping each container in
/// SHARED_CONTAINER_TYPE. Two threads doing this on the same source is a
/// data race on plain (non-atomic) memory. TSAN should report it.
TEST(GroupBitmapCowRace, ConcurrentMergeFromSharedSource)
{
    auto source = std::make_unique<BitmapWrapper32>();
    fill_wide_and_dense(*source, /*num_containers=*/500, /*values_per_container=*/100);
    ASSERT_TRUE(source->isLarge());

    constexpr size_t num_threads = 16;
    std::vector<std::unique_ptr<BitmapWrapper32>> destinations;
    destinations.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        destinations.emplace_back(std::make_unique<BitmapWrapper32>());

    /// Coordinate the start of all threads with a simple gate so they hit the
    /// shared source as simultaneously as possible.
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]()
        {
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();
            destinations[i]->merge(*source);
        });
    }
    go.store(true, std::memory_order_release);
    for (auto & t : threads)
        t.join();

    /// Functional check: every destination must equal the source value-wise.
    /// Under TSAN, the test fails earlier with a race report; without TSAN it
    /// still validates that the racy mutation did not corrupt the result.
    const auto expected_size = source->size();
    for (size_t i = 0; i < num_threads; ++i)
        EXPECT_EQ(destinations[i]->size(), expected_size) << "destination " << i;
}

/// Same setup, but instead of merge() each thread calls write() (serialize).
/// write() does `std::make_unique<RoaringBitmap>(*roaring_bitmap)` + runOptimize,
/// and the copy constructor goes through roaring_bitmap_overwrite. Under COW
/// the source's containers[]/typecodes[] are mutated. Concurrent writers on
/// the same source race.
TEST(GroupBitmapCowRace, ConcurrentSerializeOfSharedSource)
{
    auto source = std::make_unique<BitmapWrapper32>();
    fill_wide_and_dense(*source, 500, 100);
    ASSERT_TRUE(source->isLarge());

    constexpr size_t num_threads = 16;
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<std::string> outputs(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]()
        {
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();
            WriteBufferFromString buf(outputs[i]);
            source->write(buf);
            buf.finalize();
        });
    }
    go.store(true, std::memory_order_release);
    for (auto & t : threads)
        t.join();

    /// All serialized blobs must be identical - same source, same content.
    for (size_t i = 1; i < num_threads; ++i)
        EXPECT_EQ(outputs[i], outputs[0]) << "serialization " << i << " differs from 0";
}

/// Mixed workload: half the threads merge, half serialize. The same source's
/// internal containers[] / typecodes[] is the contended memory in both cases.
TEST(GroupBitmapCowRace, MixedMergeAndSerializeOnSharedSource)
{
    auto source = std::make_unique<BitmapWrapper32>();
    fill_wide_and_dense(*source, 500, 100);
    ASSERT_TRUE(source->isLarge());

    constexpr size_t num_threads = 16;
    std::vector<std::unique_ptr<BitmapWrapper32>> destinations;
    destinations.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        destinations.emplace_back(std::make_unique<BitmapWrapper32>());

    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<std::string> outputs(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]()
        {
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();
            if (i % 2 == 0)
            {
                destinations[i]->merge(*source);
            }
            else
            {
                WriteBufferFromString buf(outputs[i]);
                source->write(buf);
                buf.finalize();
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto & t : threads)
        t.join();

    const auto expected_size = source->size();
    for (size_t i = 0; i < num_threads; i += 2)
        EXPECT_EQ(destinations[i]->size(), expected_size);
}

/// Sanity / positive control: sequential merges of the same source produce
/// correct results and obviously do not race. Useful to confirm the workload
/// itself is meaningful before declaring TSAN guilty.
TEST(GroupBitmapCowRace, SequentialMergeIsFine)
{
    auto source = std::make_unique<BitmapWrapper32>();
    fill_wide_and_dense(*source, 500, 100);
    ASSERT_TRUE(source->isLarge());

    for (size_t i = 0; i < 16; ++i)
    {
        BitmapWrapper32 dst;
        dst.merge(*source);
        EXPECT_EQ(dst.size(), source->size());
    }
}

/// Reproduce the deserialization-flag entry point: build a bitmap, serialize
/// it once, deserialize into a "source" wrapper (this is the path where
/// `read()` calls `setCopyOnWrite(true)`), then race many threads on it.
/// This exercises the most common production entry point - states arriving
/// from MergeTree/network.
TEST(GroupBitmapCowRace, ConcurrentMergeAfterDeserialize32)
{
    /// Build and serialize once.
    std::string blob;
    {
        BitmapWrapper32 origin;
        fill_wide_and_dense(origin, 500, 100);
        WriteBufferFromString out(blob);
        origin.write(out);
        out.finalize();
    }

    /// Deserialize - this is the path where the PR sets COW=true on the
    /// fresh-read bitmap. That deserialized state is then "source" for all
    /// downstream merges, and contention on it is what the PR opens up.
    auto source = std::make_unique<BitmapWrapper32>();
    {
        ReadBufferFromString in(blob);
        source->read(in);
    }
    ASSERT_TRUE(source->isLarge());

    constexpr size_t num_threads = 16;
    std::vector<std::unique_ptr<BitmapWrapper32>> destinations;
    destinations.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        destinations.emplace_back(std::make_unique<BitmapWrapper32>());

    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]()
        {
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();
            destinations[i]->merge(*source);
        });
    }
    go.store(true, std::memory_order_release);
    for (auto & t : threads)
        t.join();

    const auto expected_size = source->size();
    for (size_t i = 0; i < num_threads; ++i)
        EXPECT_EQ(destinations[i]->size(), expected_size) << "destination " << i;
}

/// 64-bit (Roaring64Map) variant of ConcurrentMergeAfterDeserialize32.
/// Roaring64Map internally is std::map<uint32_t, Roaring>, so its copy/merge
/// paths walk the map and recurse into per-bucket 32-bit Roaring objects.
/// Same COW-source mutation issue applies, with extra synchronization
/// surface in the std::map machinery.
TEST(GroupBitmapCowRace, ConcurrentMergeAfterDeserialize64)
{
    /// Build and serialize once.
    std::string blob;
    {
        BitmapWrapper64 origin;
        fill_wide_and_dense(origin, UInt64(500), UInt64(100));
        WriteBufferFromString out(blob);
        origin.write(out);
        out.finalize();
    }

    /// Deserialize - this is the path where the PR sets COW=true on the
    /// fresh-read bitmap. That deserialized state is then "source" for all
    /// downstream merges, and contention on it is what the PR opens up.
    auto source = std::make_unique<BitmapWrapper64>();
    {
        ReadBufferFromString in(blob);
        source->read(in);
    }
    ASSERT_TRUE(source->isLarge());

    constexpr size_t num_threads = 16;
    std::vector<std::unique_ptr<BitmapWrapper64>> destinations;
    destinations.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        destinations.emplace_back(std::make_unique<BitmapWrapper64>());

    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]()
        {
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();
            destinations[i]->merge(*source);
        });
    }
    go.store(true, std::memory_order_release);
    for (auto & t : threads)
        t.join();

    const auto expected_size = source->size();
    for (size_t i = 0; i < num_threads; ++i)
        EXPECT_EQ(destinations[i]->size(), expected_size) << "destination " << i;
}
