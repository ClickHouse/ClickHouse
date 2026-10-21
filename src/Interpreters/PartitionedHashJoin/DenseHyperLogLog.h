#pragma once

#include <base/types.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>

namespace DB
{

/** Distinct-key estimate that sizes the one `HashJoinTable`. One sketch per fill thread. `add`
  * receives one 32-bit word per insertable build row (`computeJoinRoutesForFill`: the top 32 bits of
  * `hashJoinTableMix`, or the key itself for `key8`/`key16`). The build merges the sketches when the
  * fill ends.
  *
  * Not `HyperLogLogCounter`, although it can be fed the same words through `TrivialHash`. Its `update`
  * reads and writes a 5-bit rank through `CompactArray` (an unaligned 16-bit load, shift and mask each
  * way). On every rank increase it adjusts a floating-point denominator and a zero count. `add` here
  * runs inside the fill's row loop, so the registers stay plain bytes, one load and one store per row,
  * and all arithmetic waits for `estimate`. `estimate` also applies the large-range correction that
  * `HyperLogLogCounter::fixRawEstimate` skips above `2^32 / 30`.
  *
  * 8 KiB at precision 13, for a standard error around 1.15%, well inside the safety factor that
  * `reserveFor` multiplies the estimate by.
  */
struct DenseHyperLogLog
{
    static constexpr UInt32 precision = 13;
    static constexpr UInt32 register_count = 1u << precision;

    std::array<UInt8, register_count> registers{};

    /// Without this, the rank would read the words' low 19 bits: the middle bits of the multiplicative
    /// product, not avalanche-quality for structured keys. fmix32 is a bijection: it redistributes bits
    /// and never merges two distinct words.
    static ALWAYS_INLINE UInt32 finalize(UInt32 hash)
    {
        hash ^= hash >> 16;
        hash *= 0x85ebca6bu;
        hash ^= hash >> 13;
        hash *= 0xc2b2ae35u;
        hash ^= hash >> 16;
        return hash;
    }

    ALWAYS_INLINE void add(UInt32 hash)
    {
        const UInt32 mixed = finalize(hash);
        const UInt32 index = mixed >> (32 - precision);
        const UInt32 field = mixed & ((1u << (32 - precision)) - 1);
        const UInt8 rank = field ? static_cast<UInt8>(std::countl_zero(field) - precision + 1) : static_cast<UInt8>(32 - precision + 1);
        registers[index] = std::max(registers[index], rank);
    }

    void merge(const DenseHyperLogLog & other)
    {
        for (size_t i = 0; i < register_count; ++i)
            registers[i] = std::max(registers[i], other.registers[i]);
    }

    /// Bias-corrected harmonic mean, falling back to linear counting at low cardinality where the
    /// harmonic estimate is badly biased.
    double estimate() const
    {
        static const std::array<double, 33> inverse_powers = []
        {
            std::array<double, 33> result{};
            for (size_t rank = 0; rank < result.size(); ++rank)
                result[rank] = std::ldexp(1.0, -static_cast<int>(rank));
            return result;
        }();

        constexpr double m = register_count;
        constexpr double alpha = 0.7213 / (1.0 + 1.079 / m);
        double inverse_sum = 0;
        size_t zeros = 0;
        for (const UInt8 rank : registers)
        {
            inverse_sum += inverse_powers[rank];
            zeros += rank == 0;
        }
        const double raw = alpha * m * m / inverse_sum;
        if (raw <= 2.5 * m && zeros > 0)
            return m * std::log(m / static_cast<double>(zeros));

        /// Large-range correction. The sketch counts distinct 32-bit words, not keys (`add` sees 32 bits
        /// whatever the map hash's width). Once the estimate reaches a few percent of 2^32, birthday
        /// collisions among the words undercount the keys. Inverting `E = 2^32 * (1 - exp(-n / 2^32))`
        /// recovers `n`. Past 2^32 the sketch has saturated, and `reserveFor` clamps the reserve to the
        /// row count anyway.
        constexpr double two_32 = 4294967296.0;
        if (raw > two_32 / 30.0)
        {
            if (raw >= two_32 * 0.999)
                return two_32 * 8.0; /// saturated; `reserveFor` clamps the reserve to the row count anyway
            return -two_32 * std::log(1.0 - raw / two_32);
        }
        return raw;
    }
};

}
