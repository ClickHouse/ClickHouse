#pragma once

#include <base/types.h>
#include <base/unaligned.h>
#include <Common/HyperLogLogBiasEstimator.h>
#include <Common/CompactArray.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/transformEndianness.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}
}


/// Sets denominator type.
enum class DenominatorMode : uint8_t
{
    Compact,        /// Compact denominator.
    StableIfBig,    /// Stable denominator falling back to Compact if rank storage is not big enough.
    ExactType       /// Denominator of specified exact type.
};

namespace details
{

/// Look-up table of logarithms for integer numbers, used in HyperLogLogCounter.
template <UInt8 K>
struct LogLUT
{
    LogLUT() // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - fully assigned in the body
    {
        log_table[0] = 0.0;
        for (size_t i = 1; i <= M; ++i)
            log_table[i] = log(static_cast<double>(i));
    }

    double getLog(size_t x) const
    {
        if (x <= M)
            return log_table[x];
        return log(static_cast<double>(x));
    }

private:
    static constexpr size_t M = 1 << ((static_cast<unsigned int>(K) <= 12) ? K : 12);

    double log_table[M + 1]; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - fully assigned in constructor
};

template <UInt8 K> struct MinCounterTypeHelper;
template <> struct MinCounterTypeHelper<0>    { using Type = UInt8; };
template <> struct MinCounterTypeHelper<1>    { using Type = UInt16; };
template <> struct MinCounterTypeHelper<2>    { using Type = UInt32; };
template <> struct MinCounterTypeHelper<3>    { using Type = UInt64; };

/// Auxiliary structure for automatic determining minimum size of counter's type depending on its maximum value.
/// Used in HyperLogLogCounter in order to spend memory efficiently.
template <UInt64 MaxValue> struct MinCounterType
{
    using Type = typename MinCounterTypeHelper<
        (MaxValue >= 1 << 8) +
        (MaxValue >= 1 << 16) +
        (MaxValue >= 1ULL << 32)
        >::Type;
};

/// Denominator of expression for HyperLogLog algorithm.
template <UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType, DenominatorMode denominator_mode>
class Denominator;

/// Returns true if rank storage is big.
constexpr bool isBigRankStore(UInt8 precision)
{
    return precision >= 12;
}

/// Used to deduce denominator type depending on options provided.
template <typename HashValueType, typename DenominatorType, DenominatorMode denominator_mode>
struct IntermediateDenominator;

template <typename DenominatorType, DenominatorMode denominator_mode>
requires (denominator_mode != DenominatorMode::ExactType)
struct IntermediateDenominator<UInt32, DenominatorType, denominator_mode>
{
    using Type = double;
};

template <typename DenominatorType, DenominatorMode denominator_mode>
struct IntermediateDenominator<UInt64, DenominatorType, denominator_mode>
{
    using Type = long double;
};

template <typename HashValueType, typename DenominatorType>
struct IntermediateDenominator<HashValueType, DenominatorType, DenominatorMode::ExactType>
{
    using Type = DenominatorType;
};

/// "Lightweight" implementation of expression's denominator for HyperLogLog algorithm.
/// Uses minimum amount of memory, but estimates may be unstable.
/// Satisfiable when rank storage is small enough.
template <UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType, DenominatorMode denominator_mode>
requires (!details::isBigRankStore(precision)) || (!(denominator_mode == DenominatorMode::StableIfBig))
class __attribute__((__packed__)) Denominator<precision, max_rank, HashValueType, DenominatorType, denominator_mode>
{
private:
    using T = typename IntermediateDenominator<HashValueType, DenominatorType, denominator_mode>::Type;

public:
    Denominator(DenominatorType initial_value) /// NOLINT
        : denominator(initial_value)
    {
    }

    void update(UInt8 cur_rank, UInt8 new_rank)
    {
        denominator -= static_cast<T>(1.0) / (1ULL << cur_rank);
        denominator += static_cast<T>(1.0) / (1ULL << new_rank);
    }

    void update(UInt8 rank)
    {
        denominator += static_cast<T>(1.0) / (1ULL << rank);
    }

    void clear()
    {
        denominator = 0;
    }

    DenominatorType get() const
    {
        return denominator;
    }

private:
    T denominator;
};

/// Returns ∑ rank_count[i] * 2^-i, i < size, rounded once to T: the sum times 2^(size - 1) is an integer below 2^(32 + size).
/// The UInt32 counts may be unaligned (`Denominator` is packed).
template <typename T, size_t size>
T sumRankCountsAsInteger(const void * rank_count)
{
    static_assert(size - 1 < 64);
    using Sum = std::conditional_t<32 + size <= 64, UInt64, unsigned __int128>;
    Sum sum = 0;
    for (size_t i = 0; i < size; ++i)
        sum = (sum << 1) + unalignedLoad<UInt32>(static_cast<const char *>(rank_count) + i * sizeof(UInt32));
    static constexpr T scale = static_cast<T>(1.0 / static_cast<double>(1ULL << (size - 1)));
    return static_cast<T>(sum) * scale;
}

/// Fully-functional version of expression's denominator for HyperLogLog algorithm.
/// Spends more space that lightweight version. Estimates will always be stable.
/// Used when rank storage is big.
template <UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType, DenominatorMode denominator_mode>
requires (details::isBigRankStore(precision)) && (denominator_mode == DenominatorMode::StableIfBig)
class __attribute__((__packed__)) Denominator<precision, max_rank, HashValueType, DenominatorType, denominator_mode>
{
public:
    Denominator(DenominatorType initial_value) /// NOLINT
    {
        rank_count[0] = static_cast<UInt32>(initial_value);
    }

    void update(UInt8 cur_rank, UInt8 new_rank)
    {
        --rank_count[cur_rank];
        ++rank_count[new_rank];
    }

    void update(UInt8 rank)
    {
        ++rank_count[rank];
    }

    void clear()
    {
        memset(rank_count, 0, size * sizeof(UInt32));
    }

    /// Adds deltas[i] to the count of rank i, modulo 2^32 like `update`.
    void addToRankCounts(const Int32 * deltas)
    {
        for (size_t i = 0; i < size; ++i)
            rank_count[i] += static_cast<UInt32>(deltas[i]);
    }

    void setRankCounts(const UInt32 * counts)
    {
        memcpy(rank_count, counts, size * sizeof(UInt32));
    }

    DenominatorType get() const
    {
        /// Where long double holds the whole integer sum (eg aarch64 Linux, x86 with 32-bit hashes), the loop below is exact,
        /// so the integer sum gives the same result. Elsewhere (eg x86 with 64-bit hashes, ppc64le) the loop can round, so it stays.
        if constexpr (std::numeric_limits<long double>::is_iec559 && std::numeric_limits<long double>::digits >= 32 + size)
            return sumRankCountsAsInteger<DenominatorType, size>(rank_count);
        else
        {
            long double val = rank_count[size - 1];
            for (int i = size - 2; i >= 0; --i)
            {
                val /= 2.0L;
                val += rank_count[i];
            }
            return static_cast<DenominatorType>(val);
        }
    }

    /// Only the counts are checked, not the registers: a state whose registers disagree with its counts passes, and
    /// merging into it can make its counts wrap, so the merged state then fails this check.
    bool countsAddUp(UInt64 bucket_count, UInt64 zeros) const
    {
        UInt64 sum = 0;
        for (size_t i = 0; i < size; ++i)
            sum += rank_count[i];
        return sum == bucket_count && rank_count[0] == zeros;
    }

private:
    static constexpr size_t size = max_rank + 1;
    UInt32 rank_count[size] = { 0 };
};

/// Number of trailing zeros.
template <typename T>
struct TrailingZerosCounter;

template <>
struct TrailingZerosCounter<UInt32>
{
    static int apply(UInt32 val)
    {
        return std::countr_zero(val);
    }
};

template <>
struct TrailingZerosCounter<UInt64>
{
    static int apply(UInt64 val)
    {
        return std::countr_zero(val);
    }
};

/// Size of counter's rank in bits.
template <typename T>
struct RankWidth;

template <>
struct RankWidth<UInt32>
{
    static constexpr UInt8 get()
    {
        return 5;
    }
};

template <>
struct RankWidth<UInt64>
{
    static constexpr UInt8 get()
    {
        return 6;
    }
};

/// The maximum of two groups of 8 registers of `width` bits; bit i of `raised` is set when register i of rhs is above that of lhs.
/// Even and odd registers are compared apart, so that every field has free bits above it for the borrow of the subtraction.
template <UInt8 width>
UInt64 maxOfGroups(UInt64 lhs, UInt64 rhs, UInt64 & raised)
{
    static_assert(width >= 5 && 8 * width <= 64);
    constexpr size_t slot = 2 * width;
    constexpr UInt64 field = (1ULL << width) - 1;
    constexpr UInt64 ones = 1ULL | 1ULL << slot | 1ULL << (2 * slot) | 1ULL << (3 * slot);
    constexpr UInt64 even = ones * field;
    constexpr UInt64 guard = ones << width;

    const UInt64 lhs_even = lhs & even;
    const UInt64 lhs_odd = (lhs >> width) & even;
    const UInt64 rhs_even = rhs & even;
    const UInt64 rhs_odd = (rhs >> width) & even;

    /// The guard bit above a field survives the subtraction exactly where lhs >= rhs.
    const UInt64 less_even = (~((lhs_even | guard) - rhs_even) & guard) >> width;
    const UInt64 less_odd = (~((lhs_odd | guard) - rhs_odd) & guard) >> width;
    const UInt64 max_even = lhs_even ^ ((lhs_even ^ rhs_even) & (less_even * field));
    const UInt64 max_odd = lhs_odd ^ ((lhs_odd ^ rhs_odd) & (less_odd * field));

    /// The flags of registers 2j and 2j + 1 are at bits j * slot and j * slot + 1; the product moves them to bits 2j and 2j + 1 of one byte.
    constexpr size_t gather_shift = 3 * (slot - 2);
    constexpr UInt64 gather = 1ULL | 1ULL << (slot - 2) | 1ULL << (2 * (slot - 2)) | 1ULL << gather_shift;
    raised = (((less_even | less_odd << 1) * gather) >> gather_shift) & 0xFF;
    return max_even | max_odd << width;
}

}


/// Sets behavior of HyperLogLog class.
enum class HyperLogLogMode : uint8_t
{
    Raw,            /// No error correction.
    LinearCounting, /// LinearCounting error correction.
    BiasCorrected,  /// HyperLogLog++ error correction.
    FullFeatured    /// LinearCounting or HyperLogLog++ error correction (depending).
};

/// Estimation of number of unique values using HyperLogLog algorithm.
///
/// Theoretical relative error is ~1.04 / sqrt(2^precision), where
/// precision is size of prefix of hash-function used for indexing (number of buckets M = 2^precision).
/// Recommended values for precision are: 3..20.
///
/// Source: "HyperLogLog: The analysis of a near-optimal cardinality estimation algorithm"
/// (P. Flajolet et al., AOFA '07: Proceedings of the 2007 International Conference on Analysis
/// of Algorithms).
template <
    UInt8 precision,
    typename Key = UInt64,
    typename Hash = IntHash32<Key>,
    typename HashValueType = UInt32,
    typename DenominatorType = double,
    typename BiasEstimator = TrivialBiasEstimator,
    HyperLogLogMode mode = HyperLogLogMode::FullFeatured,
    DenominatorMode denominator_mode = DenominatorMode::StableIfBig>
class HyperLogLogCounter : private Hash
{
private:
    /// Number of buckets.
    static constexpr size_t bucket_count = 1ULL << precision;

    /// Size of counter's rank in bits.
    static constexpr UInt8 rank_width = details::RankWidth<HashValueType>::get();
    static constexpr size_t register_values = 1ULL << rank_width;
    static constexpr UInt64 register_mask = register_values - 1;

    using Value = UInt64;
    using RankStore = DB::CompactArray<HashValueType, rank_width, bucket_count>;

public:
    using value_type = Value;

    /// ALWAYS_INLINE is required to have better code layout for uniqCombined function
    void ALWAYS_INLINE insert(Value value)
    {
        HashValueType hash = getHash(value);

        /// Divide hash to two sub-values. First is bucket number, second will be used to calculate rank.
        HashValueType bucket = extractBitSequence(hash, 0, precision);
        HashValueType tail = extractBitSequence(hash, precision, sizeof(HashValueType) * 8);
        UInt8 rank = calculateRank(tail);

        /// Update maximum rank for current bucket.
        update(bucket, rank);
    }

    void ALWAYS_INLINE prefetch(Value value) const
    {
        const auto bucket = extractBitSequence(getHash(value), 0, precision);
        __builtin_prefetch(rank_store.getSerializableState().data() + static_cast<size_t>(bucket) * rank_width / 8);
    }

    UInt64 size() const
    {
        /// Normalizing factor for harmonic mean.
        static constexpr double alpha_m =
            bucket_count == 2 ? 0.351 :
            bucket_count == 4 ? 0.532 :
            bucket_count == 8 ? 0.626 :
            bucket_count == 16 ? 0.673 :
            bucket_count == 32 ? 0.697 :
            bucket_count == 64 ? 0.709 : 0.7213 / (1 + 1.079 / bucket_count);

        /// Harmonic mean for all buckets of 2^rank values is: bucket_count / ∑ 2^-rank_i,
        /// where ∑ 2^-rank_i - is denominator.

        double raw_estimate = alpha_m * bucket_count * bucket_count / denominator.get();

        double final_estimate = fixRawEstimate(raw_estimate);

        if (final_estimate < 0)
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Corrupted HyperLogLog state: the estimate {} is negative", final_estimate);
        /// For 64-bit hashes the estimate of a valid state can exceed the range of UInt64.
        if (final_estimate + 0.5 >= 0x1p64)
            return std::numeric_limits<UInt64>::max();
        return static_cast<UInt64>(final_estimate + 0.5); /// NOLINT
    }

    void merge(const HyperLogLogCounter & rhs)
    {
        if constexpr (details::isBigRankStore(precision) && denominator_mode == DenominatorMode::StableIfBig)
            mergeGroups(rhs);
        else
        {
            const auto & rhs_rank_store = rhs.rank_store;
            for (HashValueType bucket = 0; bucket < bucket_count; ++bucket)
                update</* check_rank = */ true>(bucket, rhs_rank_store[bucket]);
        }
    }

    void read(DB::ReadBuffer & in)
    {
        auto rank_state = rank_store.getSerializableState();
        in.readStrict(reinterpret_cast<char *>(rank_state.data()), rank_state.size());

        if constexpr (std::endian::native == std::endian::little)
        {
            in.readStrict(reinterpret_cast<char *>(&denominator), sizeof(DenominatorCalculatorType));
            in.readStrict(reinterpret_cast<char *>(&zeros), sizeof(ZerosCounterType));
        }
        else
        {
            constexpr size_t denom_size = sizeof(DenominatorCalculatorType);
            std::array<char, denom_size> denominator_copy;
            in.readStrict(denominator_copy.data(), denom_size);

            for (size_t i = 0; i < denominator_copy.size(); i += (sizeof(UInt32) / sizeof(char)))
            {
                UInt32 * cur = reinterpret_cast<UInt32 *>(&denominator_copy[i]);
                DB::transformEndianness<std::endian::native, std::endian::little>(*cur);
            }
            memcpy(reinterpret_cast<char *>(&denominator), denominator_copy.data(), denom_size);

            in.readStrict(reinterpret_cast<char *>(&zeros), sizeof(ZerosCounterType));
            DB::transformEndianness<std::endian::native, std::endian::little>(zeros);
        }

        if constexpr (details::isBigRankStore(precision) && denominator_mode == DenominatorMode::StableIfBig)
        {
            if (!denominator.countsAddUp(bucket_count, zeros))
                throw DB::Exception(
                    DB::ErrorCodes::INCORRECT_DATA,
                    "Corrupted HyperLogLog state: the rank counts do not add up to the number of buckets {} or the count of rank 0 "
                    "is not the stored number of zero registers {}",
                    bucket_count, zeros);
        }
    }

    static void skip(DB::ReadBuffer & in)
    {
        in.ignore(RankStore::serializedSize() + sizeof(DenominatorCalculatorType) + sizeof(ZerosCounterType));
    }

    void write(DB::WriteBuffer & out) const
    {
        auto rank_state = rank_store.getSerializableState();
        out.write(reinterpret_cast<const char *>(rank_state.data()), rank_state.size());

        if constexpr (std::endian::native == std::endian::little)
        {
            out.write(reinterpret_cast<const char *>(&denominator), sizeof(DenominatorCalculatorType));
            out.write(reinterpret_cast<const char *>(&zeros), sizeof(ZerosCounterType));
        }
        else
        {
            constexpr size_t denom_size = sizeof(DenominatorCalculatorType);
            std::array<char, denom_size> denominator_copy;
            memcpy(denominator_copy.data(), reinterpret_cast<const char *>(&denominator), denom_size);

            for (size_t i = 0; i < denominator_copy.size(); i += (sizeof(UInt32) / sizeof(char)))
            {
                UInt32 * cur = reinterpret_cast<UInt32 *>(&denominator_copy[i]);
                DB::transformEndianness<std::endian::little, std::endian::native>(*cur);
            }
            out.write(denominator_copy.data(), denom_size);

            auto zeros_copy = zeros;
            DB::transformEndianness<std::endian::little, std::endian::native>(zeros_copy);
            out.write(reinterpret_cast<const char *>(&zeros_copy), sizeof(ZerosCounterType));
        }
    }

    /// Read and write in text mode is suboptimal (but compatible with OLAPServer and Metrage).
    void readText(DB::ReadBuffer & in)
    {
        rank_store.readText(in);

        zeros = 0;
        denominator.clear();
        for (HashValueType bucket = 0; bucket < bucket_count; ++bucket)
        {
            UInt8 rank = rank_store[bucket];
            if (rank == 0)
                ++zeros;
            denominator.update(rank);
        }
    }

    static void skipText(DB::ReadBuffer & in)
    {
        UInt8 dummy = 0;
        for (size_t i = 0; i < RankStore::size(); ++i)
        {
            if (i != 0)
                DB::assertChar(',', in);
            DB::readIntText(dummy, in);
        }
    }

    void writeText(DB::WriteBuffer & out) const
    {
        rank_store.writeText(out);
    }

private:
    /// Extract subset of bits in [begin, end[ range.
    HashValueType extractBitSequence(HashValueType val, UInt8 begin, UInt8 end) const
    {
        return (val >> begin) & ((1ULL << (end - begin)) - 1);
    }

    /// Rank is number of trailing zeros.
    UInt8 calculateRank(HashValueType val) const
    {
        if (unlikely(val == 0))
            return max_rank;

        auto zeros_plus_one = details::TrailingZerosCounter<HashValueType>::apply(val) + 1;

        if (unlikely(zeros_plus_one) > max_rank)
            return max_rank;

        return static_cast<UInt8>(zeros_plus_one);
    }

    HashValueType getHash(Value key) const
    {
        /// NOTE: this should be OK, since value is the same as key for HLL.
        return static_cast<HashValueType>(
            Hash::operator()(static_cast<Key>(key)));
    }

    /// Update maximum rank for current bucket.
    /// ALWAYS_INLINE is required to have better code layout for uniqCombined function
    /// A rank from another state is checked: a corrupted state can have ranks above `max_rank`, the last index of `rank_count`.
    template <bool check_rank = false>
    void ALWAYS_INLINE update(HashValueType bucket, UInt8 rank)
    {
        UInt8 cur_rank = rank_store.get(bucket);

        if (rank > cur_rank)
        {
            if constexpr (check_rank)
            {
                if (unlikely(rank > max_rank))
                    throwRankAboveMax(rank);
            }

            if (cur_rank == 0)
                --zeros;
            denominator.update(cur_rank, rank);
            rank_store.set(bucket, rank);
        }
    }

    /// 8 registers take exactly `rank_width` bytes, a block of 8 such groups holds 64 registers.
    void mergeGroups(const HyperLogLogCounter & rhs)
    {
        static constexpr size_t block_size = 8 * rank_width;
        static constexpr size_t block_count = RankStore::serializedSize() / block_size;
        static_assert(RankStore::serializedSize() % block_size == 0);
        /// When the first blocks raise many registers, the counts are rebuilt from all the registers instead of moved one by one.
        static constexpr size_t sample_blocks = std::min<size_t>(32, block_count / 8);
        static constexpr size_t rebuild_threshold = 12 * sample_blocks;

        UInt8 * dst = rank_store.getSerializableState().data();
        const UInt8 * src = rhs.rank_store.getSerializableState().data();

        Int32 deltas[max_rank + 1] = {};
        size_t raised = 0;
        size_t block = 0;
        for (; block < sample_blocks; ++block)
            raised += mergeBlock(dst + block * block_size, src + block * block_size, deltas);

        if (raised > rebuild_threshold)
        {
            /// Four histograms, so that increments of the same rank in neighbouring registers do not wait for each other.
            /// They cover every value a register can hold.
            UInt32 histograms[4][register_values] = {};
            for (size_t b = 0; b < sample_blocks; ++b)
                for (size_t g = 0; g < 8; ++g)
                    countRanks(loadGroup(dst + b * block_size + g * rank_width), histograms);
            for (; block < block_count; ++block)
                mergeBlockAndCount(dst + block * block_size, src + block * block_size, histograms);

            UInt32 counts[register_values];
            for (size_t rank = 0; rank < register_values; ++rank)
                counts[rank] = histograms[0][rank] + histograms[1][rank] + histograms[2][rank] + histograms[3][rank];
            for (size_t rank = max_valid_rank + 1; rank < register_values; ++rank)
                if (unlikely(counts[rank] != 0))
                    throwRankAboveMax(rank);
            denominator.setRankCounts(counts);
            zeros = static_cast<ZerosCounterType>(counts[0]);
            return;
        }

        for (; block < block_count; ++block)
            mergeBlock(dst + block * block_size, src + block * block_size, deltas);
        denominator.addToRankCounts(deltas);
        zeros = static_cast<ZerosCounterType>(static_cast<Int64>(zeros) + deltas[0]);
    }

    static UInt64 loadGroup(const UInt8 * data)
    {
        UInt64 group = 0;
        memcpy(&group, data, rank_width);
        DB::transformEndianness<std::endian::native, std::endian::little>(group);
        return group;
    }

    static void storeGroup(UInt8 * data, UInt64 group)
    {
        DB::transformEndianness<std::endian::little, std::endian::native>(group);
        memcpy(data, &group, rank_width);
    }

    static void countRanks(UInt64 group, UInt32 (&histograms)[4][register_values])
    {
        for (size_t i = 0; i < 8; ++i)
            ++histograms[i % 4][(group >> (i * rank_width)) & register_mask];
    }

    [[noreturn]] static void throwRankAboveMax(UInt64 rank)
    {
        throw DB::Exception(
            DB::ErrorCodes::INCORRECT_DATA, "Corrupted HyperLogLog state: rank {} is above the maximum rank {}", rank, max_rank);
    }

    /// Merges a block of rhs into lhs, moves the counts of the registers it raises in `deltas` and returns how many it raises.
    static ALWAYS_INLINE size_t mergeBlock(UInt8 * dst, const UInt8 * src, Int32 * deltas)
    {
        UInt64 lhs_groups[8];
        UInt64 groups[8];
        UInt64 raised = 0;
        for (size_t g = 0; g < 8; ++g)
        {
            UInt64 lanes = 0;
            lhs_groups[g] = loadGroup(dst + g * rank_width);
            groups[g] = details::maxOfGroups<rank_width>(lhs_groups[g], loadGroup(src + g * rank_width), lanes);
            raised |= lanes << (g * 8);
        }
        if (raised == 0)
            return 0;

        for (size_t g = 0; g < 8; ++g)
            storeGroup(dst + g * rank_width, groups[g]);

        const size_t count = std::popcount(raised);
        while (raised)
        {
            const size_t bit = std::countr_zero(raised);
            raised &= raised - 1;
            const size_t shift = (bit % 8) * rank_width;
            const UInt64 cur_rank = (lhs_groups[bit / 8] >> shift) & register_mask;
            const UInt64 rank = (groups[bit / 8] >> shift) & register_mask;
            if (unlikely(rank > max_valid_rank))
                throwRankAboveMax(rank);
            --deltas[cur_rank];
            ++deltas[rank];
        }
        return count;
    }

    static ALWAYS_INLINE void mergeBlockAndCount(UInt8 * dst, const UInt8 * src, UInt32 (&histograms)[4][register_values])
    {
        for (size_t g = 0; g < 8; ++g)
        {
            UInt64 raised = 0;
            const UInt64 group = details::maxOfGroups<rank_width>(loadGroup(dst + g * rank_width), loadGroup(src + g * rank_width), raised);
            if (raised)
                storeGroup(dst + g * rank_width, group);
            countRanks(group, histograms);
        }
    }

    double fixRawEstimate(double raw_estimate) const
    {
        if ((mode == HyperLogLogMode::Raw) || ((mode == HyperLogLogMode::BiasCorrected) && BiasEstimator::isTrivial()))
            return raw_estimate;
        if (mode == HyperLogLogMode::LinearCounting)
            return applyLinearCorrection(raw_estimate);
        if ((mode == HyperLogLogMode::BiasCorrected) && !BiasEstimator::isTrivial())
            return applyBiasCorrection(raw_estimate);
        if (mode == HyperLogLogMode::FullFeatured)
        {
            static constexpr double pow2_32 = 4294967296.0;

            double fixed_estimate = 0;

            if (raw_estimate > (pow2_32 / 30.0))
                fixed_estimate = raw_estimate;
            else
                fixed_estimate = applyCorrection(raw_estimate);

            return fixed_estimate;
        }
        throw Poco::Exception("Internal error", DB::ErrorCodes::LOGICAL_ERROR);
    }

    double applyCorrection(double raw_estimate) const
    {
        double fixed_estimate = 0;

        if (BiasEstimator::isTrivial())
        {
            if (raw_estimate <= (2.5 * bucket_count))
            {
                /// Correction in case of small estimate.
                fixed_estimate = applyLinearCorrection(raw_estimate);
            }
            else
                fixed_estimate = raw_estimate;
        }
        else
        {
            fixed_estimate = applyBiasCorrection(raw_estimate);
            double linear_estimate = applyLinearCorrection(fixed_estimate);

            if (linear_estimate < BiasEstimator::getThreshold())
                fixed_estimate = linear_estimate;
        }

        return fixed_estimate;
    }

    /// Correction used in HyperLogLog++ algorithm.
    /// Source: "HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm"
    /// (S. Heule et al., Proceedings of the EDBT 2013 Conference).
    double applyBiasCorrection(double raw_estimate) const
    {
        double fixed_estimate = 0;

        if (raw_estimate <= (5 * bucket_count))
            fixed_estimate = raw_estimate - BiasEstimator::getBias(raw_estimate);
        else
            fixed_estimate = raw_estimate;

        return fixed_estimate;
    }

    /// Calculation of unique values using LinearCounting algorithm.
    /// Source: "A Linear-time Probabilistic Counting Algorithm for Database Applications"
    /// (Whang et al., ACM Trans. Database Syst., pp. 208-229, 1990).
    double applyLinearCorrection(double raw_estimate) const
    {
        double fixed_estimate = 0;

        if (zeros != 0)
            fixed_estimate = bucket_count * (log_lut.getLog(bucket_count) - log_lut.getLog(zeros));
        else
            fixed_estimate = raw_estimate;

        return fixed_estimate;
    }

    static constexpr int max_rank = sizeof(HashValueType) * 8 - precision + 1;
    static constexpr UInt64 max_valid_rank = max_rank;

    RankStore rank_store;

    /// Expression's denominator for HyperLogLog algorithm.
    using DenominatorCalculatorType = details::Denominator<precision, max_rank, HashValueType, DenominatorType, denominator_mode>;
    DenominatorCalculatorType denominator{bucket_count};

    /// Number of zeros in rank storage.
    using ZerosCounterType = typename details::MinCounterType<bucket_count>::Type;
    ZerosCounterType zeros = bucket_count;

    static details::LogLUT<precision> log_lut;

    /// Checks.
    static_assert(precision < (sizeof(HashValueType) * 8), "Invalid parameter value");
};


/// Declaration of static variables for linker.
template
<
    UInt8 precision,
    typename Key,
    typename Hash,
    typename HashValueType,
    typename DenominatorType,
    typename BiasEstimator,
    HyperLogLogMode mode,
    DenominatorMode denominator_mode
>
details::LogLUT<precision> HyperLogLogCounter
<
    precision,
    Key,
    Hash,
    HashValueType,
    DenominatorType,
    BiasEstimator,
    mode,
    denominator_mode
>::log_lut;


/// Lightweight implementation of expression's denominator is used in Metrage.
/// Serialization format must not be changed.
using HLL12 = HyperLogLogCounter<
    12,
    UInt64,
    IntHash32<UInt64>,
    UInt32,
    double,
    TrivialBiasEstimator,
    HyperLogLogMode::FullFeatured,
    DenominatorMode::Compact
>;
