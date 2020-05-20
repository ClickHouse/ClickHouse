#pragma once

#include <stdexcept>
#include <cmath>

#include <Core/Cuda/Types.h>
#include <Common/Cuda/CudaIntHash32.h>
#include <Common/Cuda/CudaAtomics.cuh>
#include <Common/Cuda/CudaHyperLogLogBiasEstimator.h>

/// Sets denominator type.
enum class DenominatorMode
{
    Compact,        /// Compact denominator.
    StableIfBig,    /// Stable denominator falling back to Compact if rank storage is not big enough.
    ExactType       /// Denominator of specified exact type.
};

namespace cuda_details
{

/// Look-up table of logarithms for integer numbers, used in HyperLogLogCounter.
template <DB::UInt8 K>
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

template <DB::UInt8 K> struct MinCounterTypeHelper;
template <> struct MinCounterTypeHelper<0>    { using Type = DB::UInt8; };
template <> struct MinCounterTypeHelper<1>    { using Type = DB::UInt16; };
template <> struct MinCounterTypeHelper<2>    { using Type = DB::UInt32; };
template <> struct MinCounterTypeHelper<3>    { using Type = DB::UInt64; };

/// Auxiliary structure for automatic determining minimum size of counter's type depending on its maximum value.
/// Used in HyperLogLogCounter in order to spend memory efficiently.
template <DB::UInt64 MaxValue> struct MinCounterType
{
    using Type = typename MinCounterTypeHelper<
        (MaxValue >= 1 << 8) +
        (MaxValue >= 1 << 16) +
        (MaxValue >= 1ULL << 32)
        >::Type;
};

/// Denominator of expression for HyperLogLog algorithm.
template <DB::UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
    DenominatorMode denominator_mode, typename Enable = void>
class Denominator;

namespace
{

/// Returns true if rank storage is big.
constexpr bool isBigRankStore(DB::UInt8 precision)
{
    return precision >= 12;
}

}

/// Used to deduce denominator type depending on options provided.
template <typename HashValueType, typename DenominatorType, DenominatorMode denominator_mode, typename Enable = void>
struct IntermediateDenominator;

template <typename DenominatorType, DenominatorMode denominator_mode>
struct IntermediateDenominator<DB::UInt32, DenominatorType, denominator_mode, typename std::enable_if<denominator_mode != DenominatorMode::ExactType>::type>
{
    using Type = double;
};

template <typename DenominatorType, DenominatorMode denominator_mode>
struct IntermediateDenominator<DB::UInt64, DenominatorType, denominator_mode>
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
template <DB::UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
    DenominatorMode denominator_mode>
class Denominator<precision, max_rank, HashValueType, DenominatorType,
    denominator_mode,
    typename std::enable_if<!cuda_details::isBigRankStore(precision) || !(denominator_mode == DenominatorMode::StableIfBig)>::type>
{
private:
    using T = typename IntermediateDenominator<HashValueType, DenominatorType, denominator_mode>::Type;

public:
    __device__ __host__ void initNonzeroData(DenominatorType initial_value)
    {
        denominator = initial_value;
    }
    __device__ __host__ Denominator(DenominatorType initial_value)
        : denominator(initial_value)
    {
    }

public:
    inline __device__ void update(DB::UInt8 cur_rank, DB::UInt8 new_rank)
    {
        cuda_details::atomicAdd(&denominator, 
             + static_cast<T>(1.0) / (1ULL << new_rank)
             - static_cast<T>(1.0) / (1ULL << cur_rank));
    }

    inline __device__ void update(DB::UInt8 rank)
    {
        cuda_details::atomicAdd(&denominator, 
            static_cast<T>(1.0) / (1ULL << rank));
    }

    __device__ void clear()
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
template <DB::UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
    DenominatorMode denominator_mode>
class Denominator<precision, max_rank, HashValueType, DenominatorType,
    denominator_mode,
    typename std::enable_if<cuda_details::isBigRankStore(precision) && denominator_mode == DenominatorMode::StableIfBig>::type>
{
public:
    __device__ __host__ void initNonzeroData(DenominatorType initial_value)
    {
        rank_count[0] = initial_value;
    }
    __device__ __host__ Denominator(DenominatorType initial_value)
    {
        //memset(rank_count, 0, size * sizeof(DB::UInt32));
        rank_count[0] = initial_value;
        for (uint32_t i = 1;i < size;++i) rank_count[i] = 0;
    }

    inline __device__ void update(DB::UInt8 cur_rank, DB::UInt8 new_rank)
    {
        cuda_details::atomicSub(&(rank_count[cur_rank]), (DB::UInt32)1);
        cuda_details::atomicAdd(&(rank_count[new_rank]), (DB::UInt32)1);
    }

    inline __device__ void update(DB::UInt8 rank)
    {
        cuda_details::atomicAdd(&(rank_count[rank]), (DB::UInt32)1);
    }

    __device__ void clear()
    {
        memset(rank_count, 0, size * sizeof(DB::UInt32));
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
    DB::UInt32 rank_count[size];
};

/// Number of trailing zeros.
template <typename T>
struct TrailingZerosCounter;

template <>
struct TrailingZerosCounter<DB::UInt32>
{
    static __device__ int apply(DB::UInt32 val)
    {
        return __ffs(val)-1;
    }
};

template <>
struct TrailingZerosCounter<DB::UInt64>
{
    static __device__ int apply(DB::UInt64 val)
    {
        return __ffsll(val)-1;
    }
};

/// Size of counter's rank in bits.
template <typename T>
struct RankWidth;

template <>
struct RankWidth<DB::UInt32>
{
    static constexpr DB::UInt8 get()
    {
        return 5;
    }
};

template <>
struct RankWidth<DB::UInt64>
{
    static constexpr DB::UInt8 get()
    {
        return 6;
    }
};

}

/// Sets behavior of HyperLogLog class.
enum class CudaHyperLogLogMode
{
    Raw,            /// No error correction.
    LinearCounting, /// LinearCounting error correction.
    BiasCorrected,  /// HyperLogLog++ error correction.
    FullFeatured    /// LinearCounting or HyperLogLog++ error correction (depending).
};


template <
    DB::UInt8 precision,
    typename Hash = CudaIntHash32<DB::UInt64>,
    typename HashValueType = DB::UInt32,
    typename DenominatorType = double,
    typename BiasEstimator = CudaTrivialBiasEstimator,
    CudaHyperLogLogMode mode = CudaHyperLogLogMode::FullFeatured,
    DenominatorMode denominator_mode = DenominatorMode::StableIfBig>
class CudaHyperLogLogCounter : private Hash
{
private:
    /// Number of buckets.
    static constexpr size_t bucket_count = 1ULL << precision;

    /// Size of counter's rank in bits.
    static constexpr DB::UInt8 rank_width = cuda_details::RankWidth<HashValueType>::get();

private:
    using Value_t = DB::UInt64;
    //using RankStore = DB::CompactArray<HashValueType, rank_width, bucket_count>;
    typedef DB::UInt8 RankStore[bucket_count];

public:
    __device__ __host__ void initNonzeroData()
    {
        denominator.initNonzeroData(bucket_count);
        zeros = bucket_count;
    }
    __device__ __host__ CudaHyperLogLogCounter() : denominator(bucket_count), zeros(bucket_count)
    {
        //memset(rank_store, 0, bucket_count * sizeof(DB::UInt8));
        for (uint32_t i = 0;i < bucket_count;++i) rank_store[i] = 0;
    }
    __device__ void insert(Value_t value)
    {
        HashValueType hash = getHash(value);

        /// Divide hash to two sub-values. First is bucket number, second will be used to calculate rank.
        HashValueType bucket = extractBitSequence(hash, 0, precision);
        HashValueType tail = extractBitSequence(hash, precision, sizeof(HashValueType) * 8);
        DB::UInt8 rank = calculateRank(tail);

        /// Update maximum rank for current bucket.
        update(bucket, rank);
    }

    DB::UInt64 size() const
    {
        /// Normalizing factor for harmonic mean.
        static constexpr double alpha_m =
            bucket_count == 2  ? 0.351 :
            bucket_count == 4  ? 0.532 :
            bucket_count == 8  ? 0.626 :
            bucket_count == 16 ? 0.673 :
            bucket_count == 32 ? 0.697 :
            bucket_count == 64 ? 0.709 : 0.7213 / (1 + 1.079 / bucket_count);

        /// Harmonic mean for all buckets of 2^rank values is: bucket_count / ∑ 2^-rank_i,
        /// where ∑ 2^-rank_i - is denominator.

        double raw_estimate = alpha_m * bucket_count * bucket_count / denominator.get();

        double final_estimate = fixRawEstimate(raw_estimate);

        return static_cast<DB::UInt64>(final_estimate + 0.5);
    }

    __device__ void merge(const CudaHyperLogLogCounter & rhs)
    {
        const auto & rhs_rank_store = rhs.rank_store;
        for (HashValueType bucket = 0; bucket < bucket_count; ++bucket)
            update(bucket, rhs_rank_store[bucket]);
    }

private:
    /// Extract subset of bits in [begin, end[ range.
    inline __device__ HashValueType extractBitSequence(HashValueType val, DB::UInt8 begin, DB::UInt8 end) const
    {
        return (val >> begin) & ((1ULL << (end - begin)) - 1);
    }

    /// Rank is number of trailing zeros.
    inline __device__ DB::UInt8 calculateRank(HashValueType val) const
    {
        if (val == 0)
            return max_rank;

        auto zeros_plus_one = cuda_details::TrailingZerosCounter<HashValueType>::apply(val) + 1;

        if (zeros_plus_one > max_rank)
            return max_rank;

        return zeros_plus_one;
    }

    inline __device__ HashValueType getHash(Value_t key) const
    {
        return Hash::operator()(key);
    }

    /// Update maximum rank for current bucket.
    void __device__ update(HashValueType bucket, DB::UInt8 rank)
    {
        DB::UInt8 old_rank = cuda_details::atomicMax(&(rank_store[bucket]), rank);

        if (rank > old_rank)
        {
            if (old_rank == 0)
                cuda_details::atomicSub<ZerosCounterType>(&zeros, 1);
            denominator.update(old_rank, rank);
        }
    }

    double fixRawEstimate(double raw_estimate) const
    {
        if ((mode == CudaHyperLogLogMode::Raw) || ((mode == CudaHyperLogLogMode::BiasCorrected) && BiasEstimator::isTrivial()))
            return raw_estimate;
        else if (mode == CudaHyperLogLogMode::LinearCounting)
            return applyLinearCorrection(raw_estimate);
        else if ((mode == CudaHyperLogLogMode::BiasCorrected) && !BiasEstimator::isTrivial())
            return applyBiasCorrection(raw_estimate);
        else if (mode == CudaHyperLogLogMode::FullFeatured)
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
            throw std::logic_error("CudaHyperLogLogCounter::fixRawEstimate: Internal error");
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
    using DenominatorCalculatorType = cuda_details::Denominator<precision, max_rank, HashValueType, DenominatorType, denominator_mode>;
    DenominatorCalculatorType denominator;

    /// Number of zeros in rank storage.
    using ZerosCounterType = typename cuda_details::MinCounterType<bucket_count>::Type;
    ZerosCounterType zeros;

    static cuda_details::LogLUT<precision> log_lut;

    /// Checks.
    static_assert(precision < (sizeof(HashValueType) * 8), "Invalid parameter value");
};

template
<
    DB::UInt8 precision,
    typename Hash,
    typename HashValueType,
    typename DenominatorType,
    typename BiasEstimator,
    CudaHyperLogLogMode mode,
    DenominatorMode denominator_mode
>
cuda_details::LogLUT<precision> CudaHyperLogLogCounter
<
    precision,
    Hash,
    HashValueType,
    DenominatorType,
    BiasEstimator,
    mode,
    denominator_mode
>::log_lut;
