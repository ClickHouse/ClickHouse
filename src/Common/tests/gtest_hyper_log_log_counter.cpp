#include <Common/HyperLogLogCounter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cfloat>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <pcg_random.hpp>

namespace
{

#if LDBL_MANT_DIG == 113
using Binary128 = long double;
#    define HAS_BINARY128 1
#elif defined(__SIZEOF_FLOAT128__)
using Binary128 = __float128;
#    define HAS_BINARY128 1
#endif

/// ∑ rank_count[i] * 2^-i in the order and the floating type F of the long double loop of `Denominator<StableIfBig>::get`.
template <typename F, size_t size>
double sumInOrder(const std::array<UInt32, size> & rank_count)
{
    F val = rank_count[size - 1];
    for (size_t i = size - 1; i > 0; --i)
    {
        val /= 2;
        val += rank_count[i - 1];
    }
    return static_cast<double>(val);
}

template <UInt8 K, typename HashValueType>
void checkDenominatorSize()
{
    pcg64 rng(K * 100 + sizeof(HashValueType));
    static constexpr int max_rank = sizeof(HashValueType) * 8 - K + 1;
    static constexpr size_t size = max_rank + 1;
    static constexpr UInt32 buckets = 1U << K;
    using Histogram = std::array<UInt32, size>;

    std::vector<Histogram> histograms;

    /// Consistent states: 2^K registers spread over a random window of ranks.
    for (size_t n = 0; n < 100; ++n)
    {
        size_t hi = (n % 4 == 0) ? size - 1 : rng() % size;
        size_t lo = rng() % (hi + 1);
        std::vector<UInt32> cuts{0, buckets};
        for (size_t r = lo; r < hi; ++r)
            cuts.push_back(static_cast<UInt32>(rng() % (buckets + 1)));
        std::sort(cuts.begin(), cuts.end());
        Histogram h{};
        for (size_t r = lo; r <= hi; ++r)
            h[r] = cuts[r - lo + 1] - cuts[r - lo];
        histograms.push_back(h);
    }

    /// Near-ties for the rounding of the sum: {0: 2^K - b - 2, 1: b, j: 1, max_rank: 1}.
    for (UInt32 b : {2U, 4U})
    {
        for (size_t j = 1; j < static_cast<size_t>(max_rank); ++j)
        {
            Histogram h{};
            h[0] = buckets - b - 2;
            h[1] = b;
            ++h[j];
            ++h[max_rank];
            histograms.push_back(h);
        }
    }

    /// Arbitrary counts, as a deserialized state can hold.
    for (size_t n = 0; n < 50; ++n)
    {
        Histogram h{};
        for (auto & count : h)
            count = static_cast<UInt32>(rng());
        histograms.push_back(h);
    }
    Histogram all_max{};
    all_max.fill(0xFFFFFFFF);
    histograms.push_back(all_max);
    histograms.push_back(Histogram{});
    for (size_t i = 0; i < size; ++i)
    {
        for (UInt32 count : {1U, 0xFFFFFFFFU})
        {
            Histogram h{};
            h[i] = count;
            histograms.push_back(h);
        }
    }

    /// The empty state.
    Histogram empty{};
    empty[0] = buckets;
    histograms.push_back(empty);

    for (size_t n = 0; n < histograms.size(); ++n)
    {
        const auto & h = histograms[n];
        SCOPED_TRACE(fmt::format("K = {}, hash bits = {}, histogram {}", static_cast<UInt32>(K), sizeof(HashValueType) * 8, n));

        details::Denominator<K, max_rank, HashValueType, double, DenominatorMode::StableIfBig> denominator(0);
        static_assert(sizeof(denominator) == sizeof(h));
        memcpy(static_cast<void *>(&denominator), h.data(), sizeof(h));
        EXPECT_EQ(std::bit_cast<UInt64>(denominator.get()), std::bit_cast<UInt64>(sumInOrder<long double>(h)));

#ifdef HAS_BINARY128
        /// What the long double loop computes on aarch64.
        EXPECT_EQ(
            std::bit_cast<UInt64>(details::sumRankCountsAsInteger<double, size>(h.data())),
            std::bit_cast<UInt64>(sumInOrder<Binary128>(h)));
#endif
    }
}

template <typename HashValueType, UInt8... Ks>
void checkDenominatorSizes(std::integer_sequence<UInt8, Ks...>)
{
    (checkDenominatorSize<Ks + 12, HashValueType>(), ...);
}

template <UInt8 K, typename Hash, typename HashValueType>
using Counter = HyperLogLogCounter<
    K, UInt64, Hash, HashValueType, double, TrivialBiasEstimator, HyperLogLogMode::FullFeatured, DenominatorMode::StableIfBig>;

void expectSameState(const std::string & actual, const std::string & expected)
{
    auto [actual_it, expected_it] = std::mismatch(actual.begin(), actual.end(), expected.begin(), expected.end());
    EXPECT_TRUE(actual_it == actual.end() && expected_it == expected.end())
        << "states differ at byte " << (actual_it - actual.begin()) << " of " << actual.size();
}

template <typename C>
std::string serialize(const C & counter)
{
    DB::WriteBufferFromOwnString out;
    counter.write(out);
    out.finalize();
    return out.str();
}

template <UInt8 K, typename Hash, typename HashValueType>
void checkMerge()
{
    using C = Counter<K, Hash, HashValueType>;
    pcg64 rng(K * 10 + sizeof(HashValueType));
    std::vector<UInt64> values(1ULL << (K + 3));
    for (auto & value : values)
        value = rng();

    auto build = [&](size_t begin, size_t end)
    {
        auto counter = std::make_unique<C>();
        for (size_t i = begin; i < end; ++i)
            counter->insert(values[i]);
        return counter;
    };

    const size_t n = 1ULL << (K + 2);
    const size_t sparse = 1ULL << (K - 4);
    /// {lhs begin, lhs end, rhs begin, rhs end}; the union is contiguous. Rhs raises many registers of lhs in some cases (the counts
    /// are rebuilt) and few or none in the others (the counts are moved).
    const std::vector<std::array<size_t, 4>> cases{
        {0, n, n, 2 * n},
        {0, n, 0, n / 2},
        {0, n, n / 2, n + n / 2},
        {0, sparse, sparse, 2 * sparse},
        {0, 0, 0, n},
        {0, n, 0, 0},
        {0, n, n, n + sparse}};

    for (const auto & [lhs_begin, lhs_end, rhs_begin, rhs_end] : cases)
    {
        SCOPED_TRACE(fmt::format("K = {}, hash bits = {}, lhs [{}, {}), rhs [{}, {})",
            static_cast<UInt32>(K), sizeof(HashValueType) * 8, lhs_begin, lhs_end, rhs_begin, rhs_end));
        auto lhs = build(lhs_begin, lhs_end);
        auto rhs = build(rhs_begin, rhs_end);
        lhs->merge(*rhs);
        auto expected = build(std::min(lhs_begin, rhs_begin), std::max(lhs_end, rhs_end));
        expectSameState(serialize(*lhs), serialize(*expected));
    }

    auto counter = build(0, n);
    const std::string before = serialize(*counter);
    counter->merge(*counter);
    expectSameState(serialize(*counter), before);
}

void appendLittleEndian(std::string & bytes, UInt64 value, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        bytes.push_back(static_cast<char>(value >> (8 * i)));
}

/// Serialized state: registers, the count of registers of every rank in [0, max_rank], the number of zero registers.
/// Registers above max_rank are counted as max_rank, and `moved` registers of rank 0 too, so that the counts add up as `read` checks.
template <UInt8 K, typename HashValueType>
std::string makeState(const std::vector<UInt8> & registers, UInt32 moved = 0)
{
    static constexpr size_t width = details::RankWidth<HashValueType>::get();
    static constexpr int max_rank = sizeof(HashValueType) * 8 - K + 1;
    std::string bytes((registers.size() * width + 7) / 8, '\0');
    for (size_t bucket = 0; bucket < registers.size(); ++bucket)
    {
        for (size_t bit = 0; bit < width; ++bit)
        {
            const size_t pos = bucket * width + bit;
            if (registers[bucket] & (1U << bit))
                bytes[pos / 8] = static_cast<char>(bytes[pos / 8] | (1 << (pos % 8)));
        }
    }

    std::vector<UInt32> rank_count(max_rank + 1);
    for (UInt8 rank : registers)
        ++rank_count[std::min<int>(rank, max_rank)];
    rank_count[0] -= moved;
    rank_count[max_rank] += moved;
    for (UInt32 count : rank_count)
        appendLittleEndian(bytes, count, sizeof(UInt32));
    appendLittleEndian(bytes, rank_count[0], sizeof(typename details::MinCounterType<(1ULL << K)>::Type));
    return bytes;
}

template <UInt8 K, typename HashValueType>
void checkMergeCorruptedRegisters()
{
    using C = Counter<K, TrivialHash, HashValueType>;
    static constexpr size_t width = details::RankWidth<HashValueType>::get();
    static constexpr int max_rank = sizeof(HashValueType) * 8 - K + 1;
    static constexpr size_t buckets = 1ULL << K;
    pcg64 rng(K + sizeof(HashValueType));

    auto read = [](const std::string & bytes)
    {
        auto counter = std::make_unique<C>();
        DB::ReadBufferFromString in(bytes);
        counter->read(in);
        return counter;
    };
    auto random_registers = [&](UInt32 values, size_t step)
    {
        std::vector<UInt8> registers(buckets);
        for (size_t bucket = 0; bucket < buckets; bucket += step)
            registers[bucket] = static_cast<UInt8>(rng() % values);
        return registers;
    };
    auto max_of = [](const std::vector<UInt8> & lhs, const std::vector<UInt8> & rhs)
    {
        std::vector<UInt8> result(buckets);
        for (size_t bucket = 0; bucket < buckets; ++bucket)
            result[bucket] = std::max(lhs[bucket], rhs[bucket]);
        return result;
    };

    /// lhs holds registers above max_rank and a wrong count of max_rank. When rhs raises few registers, like master, the merge
    /// moves the counts of the raised registers and keeps the rest of the counts of lhs.
    const auto corrupted = random_registers(1U << width, 4);
    const auto sparse = random_registers(max_rank + 1, 16);
    const auto dense = random_registers(max_rank + 1, 1);
    auto lhs = read(makeState<K, HashValueType>(corrupted, 7));
    lhs->merge(*read(makeState<K, HashValueType>(sparse)));
    expectSameState(serialize(*lhs), makeState<K, HashValueType>(max_of(corrupted, sparse), 7));

    /// When rhs raises many registers, the counts are rebuilt from the registers: registers above max_rank throw,
    /// wrong counts are replaced.
    lhs = read(makeState<K, HashValueType>(corrupted, 7));
    EXPECT_THROW(lhs->merge(*read(makeState<K, HashValueType>(dense))), DB::Exception);
    const auto valid = random_registers(max_rank + 1, 1);
    lhs = read(makeState<K, HashValueType>(valid, 7));
    lhs->merge(*read(makeState<K, HashValueType>(dense)));
    expectSameState(serialize(*lhs), makeState<K, HashValueType>(max_of(valid, dense)));

    /// A register above max_rank that raises lhs would move a count out of [0, max_rank].
    for (const auto & registers : {std::vector<UInt8>(buckets), dense})
    {
        auto raising = registers;
        raising.back() = max_rank + 1;
        lhs = read(makeState<K, HashValueType>(std::vector<UInt8>(buckets)));
        EXPECT_THROW(lhs->merge(*read(makeState<K, HashValueType>(raising))), DB::Exception);
    }
}

template <UInt8 width>
void checkMaxOfGroups()
{
    pcg64 rng(width);
    static constexpr UInt64 field = (1ULL << width) - 1;
    for (size_t n = 0; n < 100000; ++n)
    {
        UInt64 lhs = 0;
        UInt64 rhs = 0;
        UInt64 expected = 0;
        UInt64 expected_raised = 0;
        for (size_t i = 0; i < 8; ++i)
        {
            /// Edge values often, so that equal registers and the extremes are covered.
            const UInt64 l = (n % 3 == 0) ? (rng() % 2) * field : rng() % (field + 1);
            const UInt64 r = (n % 5 == 0) ? l : rng() % (field + 1);
            lhs |= l << (i * width);
            rhs |= r << (i * width);
            expected |= std::max(l, r) << (i * width);
            expected_raised |= static_cast<UInt64>(r > l) << i;
        }
        UInt64 raised = 0;
        ASSERT_EQ(details::maxOfGroups<width>(lhs, rhs, raised), expected) << std::hex << lhs << " " << rhs;
        ASSERT_EQ(raised, expected_raised) << std::hex << lhs << " " << rhs;
    }
}

}

TEST(HyperLogLogCounter, DenominatorSize)
{
    checkDenominatorSizes<UInt32>(std::make_integer_sequence<UInt8, 9>{});
    checkDenominatorSizes<UInt64>(std::make_integer_sequence<UInt8, 9>{});
}

TEST(HyperLogLogCounter, MaxOfGroups)
{
    checkMaxOfGroups<5>();
    checkMaxOfGroups<6>();
}

TEST(HyperLogLogCounter, Merge)
{
    checkMerge<12, IntHash32<UInt64>, UInt32>();
    checkMerge<12, TrivialHash, UInt32>();
    checkMerge<17, TrivialHash, UInt32>();
    checkMerge<12, TrivialHash, UInt64>();
    checkMerge<17, TrivialHash, UInt64>();
}

TEST(HyperLogLogCounter, MergeCorruptedRegisters)
{
    checkMergeCorruptedRegisters<12, UInt32>();
    checkMergeCorruptedRegisters<12, UInt64>();
    checkMergeCorruptedRegisters<20, UInt32>();
    checkMergeCorruptedRegisters<20, UInt64>();
}
