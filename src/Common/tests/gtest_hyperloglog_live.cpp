#include <Common/HyperLogLogCounter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>
#include <pcg_random.hpp>

namespace
{

/// The packed counter with the input and the precision of `LiveHyperLogLogCounter`.
using PackedCounter = HyperLogLogCounter<12, UInt32, FMix32Hash>;

/// The default mode keeps its layout: the counters of `uniqCombined` (K = 17), `uniqCombined64`, `uniqHLL12`, the adaptive
/// aggregation and the disk set filters. The bias estimator has no data, so `TrivialBiasEstimator` stands for the one of `uniqCombined`.
static_assert(sizeof(HyperLogLogCounter<17, UInt32, TrivialHash, UInt32>) == 81996);
static_assert(sizeof(HyperLogLogCounter<17, UInt64, TrivialHash, UInt64>) == 98508);
static_assert(sizeof(HyperLogLogCounter<12>) == 2652);
static_assert(sizeof(HyperLogLogCounter<12, UInt64, TrivialHash, UInt64>) == 3292);
static_assert(sizeof(HyperLogLogCounter<20, UInt64, TrivialHash, UInt64>) == 786624);
static_assert(std::is_copy_constructible_v<HyperLogLogCounter<17, UInt32, TrivialHash, UInt32>>);

template <typename C>
std::string serialize(const C & counter)
{
    DB::WriteBufferFromOwnString out;
    counter.write(out);
    out.finalize();
    return out.str();
}

void appendLittleEndian(std::string & bytes, UInt64 value, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        bytes.push_back(static_cast<char>(value >> (8 * i)));
}

/// Inserts the same values into every counter of the state test below. With `TrivialHash` the hash is the value: the low 12 bits
/// are the bucket, and the rank is one more than the trailing zeros of the rest, capped at the maximum rank.
template <typename C>
std::unique_ptr<C> fillThreeRegisters()
{
    auto counter = std::make_unique<C>();
    counter->insert(0);                  /// Bucket 0, no bit above the bucket: the maximum rank.
    counter->insert((1ULL << 12) | 5);   /// Bucket 5, rank 1.
    counter->insert((8ULL << 12) | 7);   /// Bucket 7, rank 4.
    counter->insert((2ULL << 12) | 7);   /// Bucket 7, rank 2: below 4, no change.
    return counter;
}

template <typename C>
void expectRoundTrip(const C & counter, const std::string & state)
{
    auto copy = std::make_unique<C>();
    DB::ReadBufferFromString in(state);
    copy->read(in);
    EXPECT_EQ(serialize(*copy), state);
    EXPECT_EQ(copy->size(), counter.size());
}

}

/// The bytes are written out by hand from the format, so that a change to the default mode that alters them fails here.
TEST(HyperLogLogCounter, PackedStateBytes)
{
    /// 32-bit hashes: 5-bit registers in a little-endian bit stream, the count of every rank in [0, 21], the number of zero
    /// registers as `UInt16`.
    {
        using C = HyperLogLogCounter<12, UInt64, TrivialHash>;
        auto counter = fillThreeRegisters<C>();
        std::string expected(2560, '\0');
        expected[0] = 21;       /// Register 0, bits 0 to 4.
        expected[3] = 1 << 1;   /// Register 5, bits 25 to 29.
        expected[4] = 4 << 3;   /// Register 7, bits 35 to 39.
        std::array<UInt32, 22> counts{};
        counts[0] = 4093;
        counts[1] = 1;
        counts[4] = 1;
        counts[21] = 1;
        for (UInt32 count : counts)
            appendLittleEndian(expected, count, 4);
        appendLittleEndian(expected, 4093, 2);
        EXPECT_EQ(serialize(*counter), expected);
        expectRoundTrip(*counter, expected);
    }

    /// 64-bit hashes: 6-bit registers, the count of every rank in [0, 53].
    {
        using C = HyperLogLogCounter<12, UInt64, TrivialHash, UInt64>;
        auto counter = fillThreeRegisters<C>();
        std::string expected(3072, '\0');
        expected[0] = 53;       /// Register 0, bits 0 to 5.
        expected[3] = 1 << 6;   /// Register 5, bits 30 to 35.
        expected[5] = 4 << 2;   /// Register 7, bits 42 to 47.
        std::array<UInt32, 54> counts{};
        counts[0] = 4093;
        counts[1] = 1;
        counts[4] = 1;
        counts[53] = 1;
        for (UInt32 count : counts)
            appendLittleEndian(expected, count, 4);
        appendLittleEndian(expected, 4093, 2);
        EXPECT_EQ(serialize(*counter), expected);
        expectRoundTrip(*counter, expected);
    }
}

TEST(LiveHyperLogLogCounter, SizeEqualsPackedSize)
{
    pcg64 rng(12);
    for (const bool sequential : {false, true})
    {
        auto live = std::make_unique<LiveHyperLogLogCounter>();
        auto packed = std::make_unique<PackedCounter>();
        ASSERT_EQ(live->size(), packed->size());
        /// Every count up to 64, then 64 checks per doubling: both sides of the linear counting threshold are covered.
        size_t next_check = 1;
        for (size_t i = 1; i <= 4'000'000; ++i)
        {
            const UInt32 value = sequential ? static_cast<UInt32>(i) : static_cast<UInt32>(rng());
            live->insert(value);
            packed->insert(value);
            if (i == next_check)
            {
                ASSERT_EQ(live->size(), packed->size()) << (sequential ? "sequential" : "random") << ", " << i << " values";
                next_check += std::max<size_t>(1, i / 64);
            }
        }
    }
}

TEST(LiveHyperLogLogCounter, ConsumeChange)
{
    auto counter = std::make_unique<LiveHyperLogLogCounter>();
    EXPECT_FALSE(counter->consumeChange());
    counter->insert(1);
    EXPECT_TRUE(counter->consumeChange());
    EXPECT_FALSE(counter->consumeChange());
    counter->insert(1);
    EXPECT_FALSE(counter->consumeChange()) << "the same value raises no register";

    auto merged = std::make_unique<LiveHyperLogLogCounter>();
    merged->mergeLive(*counter);
    EXPECT_TRUE(merged->consumeChange());
    merged->mergeLive(*counter);
    EXPECT_FALSE(merged->consumeChange());
    EXPECT_EQ(merged->size(), counter->size());
}

/// The join's live refresh: writers insert into their own counters while one thread merges the changed ones and reads the
/// estimates. Run it under TSan. The final merge then gives the estimate of one counter fed every value.
TEST(LiveHyperLogLogCounter, MergeLiveWhileInserting)
{
    static constexpr size_t writers = 4;
    static constexpr size_t values_per_writer = 1'000'000;
    /// Neighbouring writers share half of their values.
    auto value = [](size_t writer, size_t i) { return static_cast<UInt32>(intHash64(writer * values_per_writer / 2 + i)); };

    std::vector<std::unique_ptr<LiveHyperLogLogCounter>> counters;
    for (size_t writer = 0; writer < writers; ++writer)
        counters.push_back(std::make_unique<LiveHyperLogLogCounter>());
    auto merged = std::make_unique<LiveHyperLogLogCounter>();

    std::atomic<size_t> finished = 0;
    std::vector<std::jthread> threads;
    for (size_t writer = 0; writer < writers; ++writer)
    {
        threads.emplace_back(
            [&, writer]
            {
                for (size_t i = 0; i < values_per_writer; ++i)
                    counters[writer]->insert(value(writer, i));
                finished.fetch_add(1);
            });
    }

    /// Reads `size` of counters that are being written, for TSan. It is no bound: the estimate can drop where linear counting ends.
    size_t refreshes = 0;
    UInt64 largest_estimate = 0;
    while (finished.load() < writers)
    {
        for (auto & counter : counters)
        {
            largest_estimate = std::max(largest_estimate, counter->size());
            if (counter->consumeChange())
                merged->mergeLive(*counter);
        }
        largest_estimate = std::max(largest_estimate, merged->size());
        ++refreshes;
    }
    for (auto & thread : threads)
        thread.join();
    for (auto & counter : counters)
        merged->mergeLive(*counter);

    auto serial = std::make_unique<LiveHyperLogLogCounter>();
    for (size_t writer = 0; writer < writers; ++writer)
        for (size_t i = 0; i < values_per_writer; ++i)
            serial->insert(value(writer, i));
    EXPECT_EQ(merged->size(), serial->size()) << refreshes << " refreshes, largest live estimate " << largest_estimate;
}
