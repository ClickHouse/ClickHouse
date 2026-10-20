#include <Common/HashTable/Hash.h>
#include <Common/HyperLogLogCounter.h>

#include <gtest/gtest.h>

#include <atomic>
#include <charconv>
#include <cmath>
#include <memory>
#include <random>
#include <thread>
#include <vector>

namespace
{

/// The finalizer of MurmurHash3: a bijection of 32-bit words.
struct Fmix32
{
    UInt32 operator()(UInt32 x) const
    {
        x ^= x >> 16;
        x *= 0x85ebca6b;
        x ^= x >> 13;
        x *= 0xc2b2ae35;
        x ^= x >> 16;
        return x;
    }
};

using PackedCounter = HyperLogLogCounter<13, UInt32, Fmix32, UInt32>;
using ByteCounter = HyperLogLogCounter<
    13, UInt32, Fmix32, UInt32, double, TrivialBiasEstimator, HyperLogLogMode::FullFeatured, DenominatorMode::StableIfBig,
    HyperLogLogRegisters::Bytes>;
using TrackedCounter = HyperLogLogCounter<
    13, UInt32, Fmix32, UInt32, double, TrivialBiasEstimator, HyperLogLogMode::FullFeatured, DenominatorMode::StableIfBig,
    HyperLogLogRegisters::Bytes, true>;

/// The word a hash join map computes for a `UInt64` key.
UInt32 keyWord(UInt64 key)
{
    return static_cast<UInt32>(HashCRC32<UInt64>()(key));
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
constexpr size_t max_accuracy_rows = 10'000'000;
#else
constexpr size_t max_accuracy_rows = 100'000'000;
#endif

/// Inserts `rows` distinct keys and checks the estimate at every power of ten from 1000 on.
template <typename Word>
void checkAccuracy(const char * name, size_t rows, Word && word_of_row)
{
    ByteCounter counter;
    size_t next_check = 1000;
    for (size_t row = 0; row < rows; ++row)
    {
        counter.insert(word_of_row(row));
        if (row + 1 == next_check)
        {
            const double estimate = static_cast<double>(counter.size());
            const double error = std::abs(estimate - static_cast<double>(next_check)) / static_cast<double>(next_check);
            EXPECT_LE(error, 0.04) << name << ": " << next_check << " distinct keys, estimate " << estimate;
            next_check *= 10;
        }
    }
}

}

TEST(HyperLogLogCounterBytes, SameEstimateAsPacked)
{
    /// Both denominators are exact sums of powers of two, so the estimates are equal, not only close.
    PackedCounter packed;
    ByteCounter bytes;
    EXPECT_EQ(bytes.size(), UInt64{0});

    size_t next_check = 1;
    for (UInt64 key = 0; key < 3'000'000; ++key)
    {
        packed.insert(keyWord(key));
        bytes.insert(keyWord(key));
        if (key + 1 == next_check || key + 1 == 20'000 || key + 1 == 21'000)
        {
            EXPECT_EQ(bytes.size(), packed.size()) << key + 1 << " keys";
            next_check *= 3;
        }
    }
}

TEST(HyperLogLogCounterBytes, Accuracy)
{
    checkAccuracy("sequential", max_accuracy_rows, [](size_t row) { return keyWord(row); });
    checkAccuracy("strided", max_accuracy_rows, [](size_t row) { return keyWord(static_cast<UInt64>(row) << 20); });

    /// Distinct random keys: a bijection of the row number.
    checkAccuracy("random", max_accuracy_rows, [](size_t row) { return keyWord(intHash64(row)); });

    /// The counter sees the 32-bit hash of the map, so it counts the distinct hashes, not the distinct keys. For 1e8
    /// distinct strings `key_<n>` there are only 93.8 million distinct hashes, so strings stop at 1e7.
    char buffer[32] = "key_";
    checkAccuracy(
        "string",
        10'000'000,
        [&buffer](size_t row)
        {
            const auto * end = std::to_chars(buffer + 4, buffer + sizeof(buffer), row).ptr;
            return static_cast<UInt32>(StringViewHash()(std::string_view(buffer, end)));
        });
}

TEST(HyperLogLogCounterBytes, MergeLiveEqualsMergeAfterWritersStop)
{
    ByteCounter writer;
    for (UInt64 key = 0; key < 100'000; ++key)
        writer.insert(keyWord(key));

    ByteCounter merged;
    merged.merge(writer);
    ByteCounter merged_live;
    merged_live.mergeLive(writer);
    EXPECT_EQ(merged_live.size(), merged.size());
    EXPECT_EQ(merged.size(), writer.size());
}

TEST(HyperLogLogCounterBytes, TakeChanged)
{
    TrackedCounter counter;
    EXPECT_FALSE(counter.takeChanged());

    /// Every rank is at least 1, so the first insert into a bucket raises it.
    counter.insert(keyWord(1));
    EXPECT_TRUE(counter.takeChanged());
    EXPECT_FALSE(counter.takeChanged());

    counter.insert(keyWord(1));
    EXPECT_FALSE(counter.takeChanged());
}

namespace
{

/// Each writer inserts its own keys into its own counter while one reader merges them with `mergeLive` whenever
/// `takeChanged` says so. After the writers stop, one more round of the reader must see every rise.
void checkLiveReader(size_t writers, size_t keys_per_writer)
{
    std::vector<std::unique_ptr<TrackedCounter>> counters(writers);
    std::vector<std::atomic<TrackedCounter *>> published(writers);

    std::atomic<size_t> running = writers;
    std::vector<std::thread> threads;
    for (size_t writer = 0; writer < writers; ++writer)
    {
        threads.emplace_back(
            [&, writer]
            {
                /// The counter is created by its writer, as a join stream creates its sketch at its first block.
                counters[writer] = std::make_unique<TrackedCounter>();
                counters[writer]->insert(keyWord(writer * keys_per_writer));
                published[writer].store(counters[writer].get(), std::memory_order_release);
                for (size_t key = 1; key < keys_per_writer; ++key)
                    counters[writer]->insert(keyWord(writer * keys_per_writer + key));
                running.fetch_sub(1, std::memory_order_release);
            });
    }

    TrackedCounter live;
    auto merge_changed = [&]
    {
        for (auto & slot : published)
        {
            TrackedCounter * counter = slot.load(std::memory_order_acquire);
            if (counter && counter->takeChanged())
                live.mergeLive(*counter);
        }
    };
    while (running.load(std::memory_order_acquire) != 0)
        merge_changed();

    for (auto & thread : threads)
        thread.join();
    merge_changed();

    TrackedCounter expected;
    for (const auto & counter : counters)
        expected.merge(*counter);
    EXPECT_EQ(live.size(), expected.size());

    const double error = std::abs(static_cast<double>(live.size()) - static_cast<double>(writers * keys_per_writer))
        / static_cast<double>(writers * keys_per_writer);
    EXPECT_LE(error, 0.04);
}

}

TEST(HyperLogLogCounterBytes, LiveReaderOneWriter)
{
    checkLiveReader(1, 1'000'000);
}

TEST(HyperLogLogCounterBytes, LiveReaderEightWriters)
{
    checkLiveReader(8, 1'000'000);
}
