#pragma once

#include <common/types.h>
#include <Common/HyperLogLogBiasEstimator.h>
#include <Common/CompactArray.h>
#include <Common/HashTable/Hash.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>

#include <cmath>
#include <cstring>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}


/// Sets denominator type.
enum class DenominatorMode
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
    LogLUT()
    {
        log_table[0] = 0.0;
        for (size_t i = 1; i <= M; ++i)
            log_table[i] = log(static_cast<double>(i));
    }

    double getLog(size_t x) const
    {
        if (x <= M)
            return log_table[x];
        else
            return log(static_cast<double>(x));
    }

private:
    static constexpr size_t M = 1 << ((static_cast<unsigned int>(K) <= 12) ? K : 12);

    double log_table[M + 1];
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
template <UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
    DenominatorMode denominator_mode, typename Enable = void>
class Denominator;

/// Returns true if rank storage is big.
constexpr bool isBigRankStore(UInt8 precision)
{
    return precision >= 12;
}

/// Used to deduce denominator type depending on options provided.
template <typename HashValueType, typename DenominatorType, DenominatorMode denominator_mode, typename Enable = void>
struct IntermediateDenominator;

template <typename DenominatorType, DenominatorMode denominator_mode>
struct IntermediateDenominator<UInt32, DenominatorType, denominator_mode, std::enable_if_t<denominator_mode != DenominatorMode::ExactType>>
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
template <UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
    DenominatorMode denominator_mode>
class __attribute__((__packed__)) Denominator<precision, max_rank, HashValueType, DenominatorType,
    denominator_mode,
    std::enable_if_t<!details::isBigRankStore(precision) || !(denominator_mode == DenominatorMode::StableIfBig)>>
{
private:
    using T = typename IntermediateDenominator<HashValueType, DenominatorType, denominator_mode>::Type;

public:
    Denominator(DenominatorType initial_value)
        : denominator(initial_value)
    {
    }

public:
    inline void update(UInt8 cur_rank, UInt8 new_rank)
    {
        denominator -= static_cast<T>(1.0) / (1ULL << cur_rank);
        denominator += static_cast<T>(1.0) / (1ULL << new_rank);
    }

    inline void update(UInt8 rank)
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

/// Fully-functional version of expression's denominator for HyperLogLog algorithm.
/// Spends more space that lightweight version. Estimates will always be stable.
/// Used when rank storage is big.
template <UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
    DenominatorMode denominator_mode>
class __attribute__((__packed__)) Denominator<precision, max_rank, HashValueType, DenominatorType,
    denominator_mode,
    std::enable_if_t<details::isBigRankStore(precision) && denominator_mode == DenominatorMode::StableIfBig>>
{
public:
    Denominator(DenominatorType initial_value)
    {
        rank_count[0] = initial_value;
    }

    inline void update(UInt8 cur_rank, UInt8 new_rank)
    {
        --rank_count[cur_rank];
        ++rank_count[new_rank];
    }

    inline void update(UInt8 rank)
    {
        ++rank_count[rank];
    }

    void clear()
    {
        memset(rank_count, 0, size * sizeof(UInt32));
    }

    DenominatorType get() const
    {
        long double val = rank_count[size - 1];
        for (int i = size - 2; i >= 0; --i)
        {
            val /= 2.0;
            val += rank_count[i];
        }
        return val;
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
        return __builtin_ctz(val);
    }
};

template <>
struct TrailingZerosCounter<UInt64>
{
    static int apply(UInt64 val)
    {
        return __builtin_ctzll(val);
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

}


/// Sets behavior of HyperLogLog class.
enum class HyperLogLogMode
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
    typename Hash = IntHash32<UInt64>,
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

        return static_cast<UInt64>(final_estimate + 0.5);
    }

    void merge(const HyperLogLogCounter & rhs)
    {
        const auto & rhs_rank_store = rhs.rank_store;
        for (HashValueType bucket = 0; bucket < bucket_count; ++bucket)
            update(bucket, rhs_rank_store[bucket]);
    }

    void read(DB::ReadBuffer & in)
    {
        in.readStrict(reinterpret_cast<char *>(this), sizeof(*this));
    }

    void readAndMerge(DB::ReadBuffer & in)
    {
        typename RankStore::Reader reader(in);
        while (reader.next())
        {
            const auto & data = reader.get();
            update(data.first, data.second);
        }

        in.ignore(sizeof(DenominatorCalculatorType) + sizeof(ZerosCounterType));
    }

    static void skip(DB::ReadBuffer & in)
    {
        in.ignore(sizeof(RankStore) + sizeof(DenominatorCalculatorType) + sizeof(ZerosCounterType));
    }

    void write(DB::WriteBuffer & out) const
    {
        out.write(reinterpret_cast<const char *>(this), sizeof(*this));
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
        UInt8 dummy;
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
    inline HashValueType extractBitSequence(HashValueType val, UInt8 begin, UInt8 end) const
    {
        return (val >> begin) & ((1ULL << (end - begin)) - 1);
    }

    /// Rank is number of trailing zeros.
    inline UInt8 calculateRank(HashValueType val) const
    {
        if (unlikely(val == 0))
            return max_rank;

        auto zeros_plus_one = details::TrailingZerosCounter<HashValueType>::apply(val) + 1;

        if (unlikely(zeros_plus_one) > max_rank)
            return max_rank;

        return zeros_plus_one;
    }

    inline HashValueType getHash(Value key) const
    {
        return Hash::operator()(key);
    }

    /// Update maximum rank for current bucket.
    /// ALWAYS_INLINE is required to have better code layout for uniqCombined function
    void ALWAYS_INLINE update(HashValueType bucket, UInt8 rank)
    {
        typename RankStore::Locus content = rank_store[bucket];
        UInt8 cur_rank = static_cast<UInt8>(content);

        if (rank > cur_rank)
        {
            if (cur_rank == 0)
                --zeros;
            denominator.update(cur_rank, rank);
            content = rank;
        }
    }

    double fixRawEstimate(double raw_estimate) const
    {
        if ((mode == HyperLogLogMode::Raw) || ((mode == HyperLogLogMode::BiasCorrected) && BiasEstimator::isTrivial()))
            return raw_estimate;
        else if (mode == HyperLogLogMode::LinearCounting)
            return applyLinearCorrection(raw_estimate);
        else if ((mode == HyperLogLogMode::BiasCorrected) && !BiasEstimator::isTrivial())
            return applyBiasCorrection(raw_estimate);
        else if (mode == HyperLogLogMode::FullFeatured)
        {
            static constexpr double pow2_32 = 4294967296.0;

            double fixed_estimate;

            if (raw_estimate > (pow2_32 / 30.0))
                fixed_estimate = raw_estimate;
            else
                fixed_estimate = applyCorrection(raw_estimate);

            return fixed_estimate;
        }
        else
            throw Poco::Exception("Internal error", DB::ErrorCodes::LOGICAL_ERROR);
    }

    inline double applyCorrection(double raw_estimate) const
    {
        double fixed_estimate;

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
    inline double applyBiasCorrection(double raw_estimate) const
    {
        double fixed_estimate;

        if (raw_estimate <= (5 * bucket_count))
            fixed_estimate = raw_estimate - BiasEstimator::getBias(raw_estimate);
        else
            fixed_estimate = raw_estimate;

        return fixed_estimate;
    }

    /// Calculation of unique values using LinearCounting algorithm.
    /// Source: "A Linear-time Probabilistic Counting Algorithm for Database Applications"
    /// (Whang et al., ACM Trans. Database Syst., pp. 208-229, 1990).
    inline double applyLinearCorrection(double raw_estimate) const
    {
        double fixed_estimate;

        if (zeros != 0)
            fixed_estimate = bucket_count * (log_lut.getLog(bucket_count) - log_lut.getLog(zeros));
        else
            fixed_estimate = raw_estimate;

        return fixed_estimate;
    }

private:
    static constexpr int max_rank = sizeof(HashValueType) * 8 - precision + 1;

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
    IntHash32<UInt64>,
    UInt32,
    double,
    TrivialBiasEstimator,
    HyperLogLogMode::FullFeatured,
    DenominatorMode::Compact
>;
