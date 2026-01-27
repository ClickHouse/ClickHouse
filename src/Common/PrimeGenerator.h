#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// The following query will hit many edge cases in prime generation:
/// SELECT * FROM system.primes WHERE prime == toUInt64('18446744073709551557');
/// Useful to check after making changes to the prime generation code.

/// Return floor(sqrt(x)) for UInt64.
///
/// We cannot just do `static_cast<UInt64>(std::sqrt(x))` because `std::sqrt` operates on floating point values and can
/// round up/down for large `UInt64` inputs. Many parts of the sieve rely on exact integer boundaries (e.g. primes up to
/// sqrt(segment_end)), so we take a floating-point approximation and then adjust it with integer arithmetic until
/// `r*r <= x < (r+1)*(r+1)` (checked via divisions to avoid overflow).
inline UInt64 integerSqrt(UInt64 x)
{
    UInt64 r = static_cast<UInt64>(std::sqrt(static_cast<long double>(x)));
    while (r > 0 && r > x / r)
        --r;
    while (r + 1 > r && (r + 1) <= x / (r + 1))
        ++r;
    return r;
}

/// Ensures that primes up to a certain limit are calculated excluding 2.
/// For example, if `ensureUpTo(100)` is called, all primes up to 100 will be calculated and cached.
class OddPrimesCache
{
public:
    /// Seed the cache with an initial small, valid odd-only sieve [3..7]. Then, we can start extending it on demand.
    ///
    /// We seed up to 7 (not just 3) so that there are fewer sqrt-recursion steps in some cases.
    /// For example, `ensureUpTo(1000000)` will round the limit down to 999999 and walk this recursion chain:
    /// - seed [3..7]: `999999 -> 999 -> 31`
    /// - seed [3]: `999999 -> 999 -> 31 -> 5`
    OddPrimesCache()
        : current_limit(7)
        , composite_bits(1, 0)
        , primes{3, 5, 7}
    {
#ifndef NDEBUG
        chassert((current_limit & 1) == 1);
        chassert(current_limit >= 3);

        const UInt64 num_odds = ((current_limit - 3) >> 1) + 1;
        chassert(num_odds == 3);
        chassert(composite_bits.size() == (num_odds + 63) / 64);

        auto is_composite = [&](UInt64 idx) { return (composite_bits[idx >> 6] >> (idx & 63)) & 1ULL; };
        chassert(!is_composite(0) && "expected number 3 to be prime");
        chassert(!is_composite(1) && "expected number 5 to be prime");
        chassert(!is_composite(2) && "expected number 7 to be prime");

        chassert(composite_bits[0] == 0);

        chassert(primes.size() == 3);
        chassert(primes[0] == 3);
        chassert(primes[1] == 5);
        chassert(primes[2] == 7);
        chassert(std::is_sorted(primes.begin(), primes.end()));
#endif
    }

    /// Ensure that the cache contains all odd primes up to `limit` (inclusive).
    ///
    /// The cache stores only odd numbers starting from 3, so bit index `i` corresponds to the value `3 + 2*i`.
    /// The cache is seeded with [3, 5, 7] and then extended on demand.
    ///
    /// High-level steps:
    /// - Round `limit` down to an odd number and validate it is within supported range.
    /// - Fast-path: return if the cache already covers the requested limit.
    /// - Extend the sieve range and mark composites using already known base primes.
    void ensureUpTo(UInt64 limit)
    {
        /// 2 is not covered by this cache
        if (limit < 3)
            return;

        /// Make limit odd
        UInt64 limit_odd = limit;
        if ((limit_odd & 1) == 0)
            --limit_odd;

        if (limit_odd > std::numeric_limits<UInt32>::max())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "OddPrimesCache supports limit up to {}, got {}",
                static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1,
                limit);

        /// We already have enough primes cached
        if (limit_odd <= current_limit)
            return;

        const UInt64 sqrt_limit = integerSqrt(limit_odd);

        /// Ensure primes up to sqrt(limit_odd) exist before sieving the extension
        if (sqrt_limit >= 3 && current_limit < sqrt_limit)
            ensureUpTo(sqrt_limit);

        /// Extend existing sieve [3..current_limit] -> [3..limit_odd].
        const UInt64 old_limit = current_limit;
        const UInt64 old_n_odds = ((old_limit - 3) >> 1) + 1;

        current_limit = limit_odd;
        const UInt64 new_n_odds = ((current_limit - 3) >> 1) + 1;

        composite_bits.resize((new_n_odds + 63) / 64, 0);

        const UInt64 first_new_odd = old_limit + 2;

        /// At this point, we are guaranteed that primes contains all primes up to sqrt_limit
        /// Mark the multiples of existing primes in the new range
        for (UInt64 prime : primes)
        {
            if (prime > sqrt_limit)
                break;

            UInt64 prime_sq = prime * prime;

            /// Find the first multiple of prime in [first_new_odd..current_limit]
            if (prime_sq < first_new_odd)
            {
                const UInt64 rem = first_new_odd % prime;
                prime_sq = rem ? (first_new_odd + (prime - rem)) : first_new_odd;
            }

            /// Make sure we start with an odd multiple
            if ((prime_sq & 1) == 0)
                prime_sq += prime;

            const UInt64 multiple_start_index = (prime_sq - 3) >> 1;

            /// Mark odd multiples of prime as composite
            for (UInt64 idx = multiple_start_index; idx < new_n_odds; idx += prime)
                setBit(composite_bits, idx);
        }

        for (UInt64 i = old_n_odds; i < new_n_odds; ++i)
            if (!getBit(composite_bits, i))
                primes.push_back(static_cast<UInt32>(3 + (i << 1)));

        chassert(std::is_sorted(primes.begin(), primes.end()));
    }

    const std::vector<UInt32> & getPrimes() const { return primes; }

private:
    /// Marks or gets the bit corresponding to odd number at index idx (0 -> 3, 1 -> 5, 2 -> 7, ...)
    /// 1 for composite, 0 for prime
    static inline bool getBit(const std::vector<UInt64> & bits, UInt64 idx) { return (bits[idx >> 6] >> (idx & 63)) & 1ULL; }
    static inline void setBit(std::vector<UInt64> & bits, UInt64 idx) { bits[idx >> 6] |= (1ULL << (idx & 63)); }

    /// Current limit up to which primes are cached (odd).
    UInt64 current_limit = 1;

    /// Odd-only bits for [3, 5, 7, ..., current_limit] whether composite
    std::vector<UInt64> composite_bits;

    /// Contains all odd primes up to current_limit
    std::vector<UInt32> primes;
};

/// Given an odd number range, it generates primes in the blocks of SEGMENT_SIZE reducing memory usage.
/// It only holds the bitmap for one segment at a time that contains whether odd numbers in that segment are composite or not.
/// That bitmap is itself compressed using 64-bit words.
class SegmentedOddSieve
{
public:
    /// Segment span in integers (full segment covers `SEGMENT_SIZE + 1` consecutive integers)
    static constexpr UInt64 SEGMENT_SIZE = (1ULL << 19);

    /// How many odd numbers can fit in a segment
    static constexpr UInt64 SEGMENT_ODD_CAPACITY = (SEGMENT_SIZE >> 1) + 1;

    /// How many 64-bit words are needed to represent the odd numbers in a segment
    static constexpr size_t SEGMENT_WORD_CAPACITY = (SEGMENT_ODD_CAPACITY + 63) / 64;

    /// A special value in the event a multiple of prime might overflow beyond UInt64
    static constexpr UInt64 NO_MULTIPLE = 0;

    static constexpr UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();

    static_assert((MAX_UINT64 & 1) == 1, "MAX_UINT64 must be odd");

    void resetEmpty()
    {
        bounded = true;
        range_low_odd = 0;
        range_high_odd = 0;

        resetSegmentState();
    }

    void resetUnbounded()
    {
        bounded = false;
        range_low_odd = 3;
        range_high_odd = MAX_UINT64; /// 2^64-1 is odd

        resetSegmentState();
    }

    /// low_odd/high_odd must be odd, >= 3, and low_odd <= high_odd.
    void resetRange(UInt64 low_odd, UInt64 high_odd)
    {
        if (low_odd < 3 || high_odd < 3 || (low_odd & 1) == 0 || (high_odd & 1) == 0 || low_odd > high_odd)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid arguments to SegmentedOddSieve::resetRange({}, {}). "
                "Both must be odd and >= 3, and low_odd <= high_odd",
                low_odd,
                high_odd);
        bounded = true;
        range_low_odd = low_odd;
        range_high_odd = high_odd;

        resetSegmentState();
    }

    /// Return the next odd prime in the configured range.
    ///
    /// The sieve is segmented: at any point, `segment_composite_bits` holds compositeness for one segment of consecutive
    /// odd numbers, and `next_candidate_idx` tracks where scanning should continue.
    ///
    /// High-level steps:
    /// - Ensure the current segment is sieved (call `sieveNextSegment()` if needed).
    /// - Scan the bitmap starting at `next_candidate_idx` for the next non-composite bit (prime).
    /// - Convert the found index to a value (`segment_begin + 2*idx`), advance `next_candidate_idx`, and return it.
    /// - If the segment has no remaining primes, advance to the next segment and continue.
    ///
    /// Returns `std::nullopt` only when the bounded range is exhausted (or if the unbounded stream overflows, which is
    /// infeasible in practice).
    std::optional<UInt64> next()
    {
        while (true)
        {
            /// Emitted all primes in the current segment. Sieve a new segment if needed
            if (!segment_sieved || next_candidate_idx >= segment_odd_count)
            {
                if (!sieveNextSegment())
                    return std::nullopt;
            }

            chassert(segment_sieved);
            chassert((segment_begin & 1) == 1);
            chassert(segment_odd_count > 0);
            chassert(segment_word_count > 0);
            chassert(next_candidate_idx < segment_odd_count);
            chassert(segment_composite_bits.size() == SEGMENT_WORD_CAPACITY);

            /// A word contains 64 bits for 64 odd numbers
            UInt64 word = next_candidate_idx >> 6;
            UInt64 bit = next_candidate_idx & 63;

            while (word < segment_word_count)
            {
                UInt64 word_bits = ~segment_composite_bits[word];
                word_bits &= (~UInt64(0) << bit);

                if (word_bits)
                {
                    const unsigned t = __builtin_ctzll(word_bits);
                    const UInt64 found = (word << 6) + t;

                    /// Tail bits are padded as composite
                    chassert(found < segment_odd_count);

                    next_candidate_idx = found + 1;
                    return segment_begin + (found << 1); /// convert index back to odd number
                }

                /// No more primes in this word; advance to next word
                ++word;
                bit = 0;
            }

            /// No primes in this segment; force next segment
            next_candidate_idx = segment_odd_count;
        }
    }

private:
    void resetSegmentState()
    {
        segment_sieved = false;
        segment_begin = 0;
        segment_odd_count = 0;
        segment_word_count = 0;
        next_candidate_idx = 0;

        active_primes = 0;
        next_multiple_by_prime.clear();
    }

    void markCompositeInSegment(UInt64 n)
    {
#ifndef NDEBUG
        const UInt64 segment_end = segment_begin + ((segment_odd_count - 1) << 1);

        chassert((segment_begin & 1) == 1);
        chassert((segment_end & 1) == 1);
        chassert((n & 1) == 1);
        chassert(n >= segment_begin && n <= segment_end);
#endif

        chassert(((n - segment_begin) & 1) == 0);
        const UInt64 idx = (n - segment_begin) >> 1;
        chassert(idx < segment_odd_count);
        segment_composite_bits[idx >> 6] |= (1ULL << (idx & 63));
    }

    /// Prepare and sieve the next segment of odd numbers.
    ///
    /// The sieve stores only odd values and represents a segment by a bitmap where bit `i` corresponds to the odd number
    /// `segment_begin + 2*i` and is set for composites.
    ///
    /// High-level steps:
    /// - Choose `segment_begin` for the first segment, otherwise advance to the next odd after the previous segment end.
    /// - Compute the segment end (clamped to `range_high_odd`) and derive `segment_odd_count`/`segment_word_count`.
    /// - Clear the bitmap and call `sieveSegment()` to mark composites using base primes.
    /// - Pad tail bits and reset `next_candidate_idx` for scanning primes in `next()`.
    ///
    /// Returns false when the bounded range is exhausted or when `UInt64` overflow prevents advancing.
    bool sieveNextSegment()
    {
        static_assert((SEGMENT_SIZE & 1) == 0, "SEGMENT_SIZE must be even");

        if (bounded)
        {
            if (range_high_odd == 0)
                return false;
        }

        chassert(bounded || (range_low_odd == 3 && range_high_odd == MAX_UINT64));

        if (!segment_sieved)
        {
            segment_begin = range_low_odd;
            segment_sieved = true;
        }
        else
        {
            const UInt64 advance = (segment_odd_count << 1); /// next begin = prev_end + 2

            if (unlikely(segment_begin > MAX_UINT64 - advance)) /// Overflow beyond UInt64
                return false;

            segment_begin += advance;
        }

        chassert(segment_begin != 0);

        /// We already finished the last segment in the bounded range
        if (bounded && segment_begin > range_high_odd)
            return false;

        /// Compute segment_end = min(segment_begin + SEGMENT_SIZE, range_high_odd), overflow-safe.
        UInt64 segment_end = segment_begin + SEGMENT_SIZE;
        if (unlikely(segment_end < segment_begin))
            segment_end = MAX_UINT64; /// clamp on overflow (MAX_UINT64 is odd)
        segment_end = std::min(segment_end, range_high_odd);

        chassert((segment_begin & 1) == 1);
        chassert((segment_end & 1) == 1);
        chassert(segment_begin <= segment_end);

        segment_odd_count = ((segment_end - segment_begin) >> 1) + 1;
        segment_word_count = (segment_odd_count + 63) / 64;

        chassert(segment_odd_count > 0);
        chassert(segment_odd_count <= SEGMENT_ODD_CAPACITY);
        chassert(segment_word_count > 0);
        chassert(segment_word_count <= SEGMENT_WORD_CAPACITY);

        /// To avoid frequent reallocations, we reserve SEGMENT_WORD_CAPACITY words
        if (segment_composite_bits.size() != SEGMENT_WORD_CAPACITY)
            segment_composite_bits.assign(SEGMENT_WORD_CAPACITY, 0);
        else
            std::fill(segment_composite_bits.begin(), segment_composite_bits.begin() + segment_word_count, 0);

        sieveSegment();

        /// Pad tail bits of the last word so scanning never returns out-of-range positions
        const unsigned tail_bits = static_cast<unsigned>(segment_odd_count & 63);
        if (tail_bits != 0)
            segment_composite_bits[segment_word_count - 1] |= (~0ULL) << tail_bits;

        next_candidate_idx = 0;
        return true;
    }

    /// Mark composite numbers in the current segment bitmap.
    ///
    /// Assumes that `segment_begin`/`segment_odd_count` describe the current segment and `segment_composite_bits` was cleared.
    ///
    /// High-level steps:
    /// - Compute `segment_end` from segment state.
    /// - Ensure base primes up to `sqrt(segment_end)` exist via `OddPrimesCache`.
    /// - For each active base prime `p`, mark odd multiples `m` in this segment (`m += 2*p`) in the bitmap; keep the first
    ///   multiple after `segment_end` in `next_multiple_by_prime` to resume quickly in the next segment.
    void sieveSegment()
    {
        chassert(segment_odd_count > 0);
        const UInt64 segment_end = segment_begin + ((segment_odd_count - 1) << 1);

        const UInt64 sqrt_hi = integerSqrt(segment_end);
        primes_cache.ensureUpTo(sqrt_hi);

        const auto & base_primes = primes_cache.getPrimes();

        chassert(std::is_sorted(base_primes.begin(), base_primes.end()));

        /// A sqrt of any UInt64 fits in UInt32
        const UInt32 sqrt_hi_u32 = static_cast<UInt32>(sqrt_hi);

        /// Activate newly needed base primes (monotonic across segments)
        while (active_primes < base_primes.size() && base_primes[active_primes] <= sqrt_hi_u32)
        {
            const UInt64 prime = base_primes[active_primes];
            const UInt64 step = prime << 1; /// prime * 2 (odd multiples only)

            UInt64 prime_sq = prime * prime; /// Odd

            if (prime_sq < segment_begin)
            {
                const UInt64 delta = segment_begin - prime_sq;

                /// k = ceil(delta / step) without overflow
                UInt64 k = delta / step;
                if (delta % step)
                    ++k;

                if (unlikely(k > (MAX_UINT64 - prime_sq) / step))
                    prime_sq = NO_MULTIPLE;
                else
                    prime_sq += k * step;
            }

            chassert(prime_sq == NO_MULTIPLE || ((prime_sq & 1) == 1 && prime_sq >= segment_begin));

            next_multiple_by_prime.push_back(prime_sq);
            ++active_primes;
        }

        chassert(next_multiple_by_prime.size() == active_primes);

        /// Mark composites
        for (size_t i = 0; i < active_primes; ++i)
        {
            const UInt64 prime = base_primes[i];
            const UInt64 step = prime << 1;

            UInt64 prime_sq = next_multiple_by_prime[i];
            if (unlikely(prime_sq == NO_MULTIPLE))
                continue;

            chassert((prime_sq & 1) == 1);
            chassert(prime_sq >= segment_begin);

            if (likely(segment_end <= MAX_UINT64 - step))
            {
                for (; prime_sq <= segment_end; prime_sq += step)
                    markCompositeInSegment(prime_sq);
            }
            else
            {
                const UInt64 max_before_overflow = MAX_UINT64 - step;
                while (prime_sq <= segment_end)
                {
                    markCompositeInSegment(prime_sq);
                    if (prime_sq > max_before_overflow)
                    {
                        prime_sq = NO_MULTIPLE;
                        break;
                    }
                    prime_sq += step;
                }
            }

            next_multiple_by_prime[i] = prime_sq; /// First multiple > segment_end, or NO_MULTIPLE if it would overflow
        }
    }

    /// Whether we generate primes only within a fixed range.
    /// Updated by `resetEmpty()`/`resetUnbounded()`/`resetRange()`, used by `sieveNextSegment()` to decide when to stop.
    bool bounded = false;

    /// Inclusive range bounds for prime generation (odd values >= 3 when non-empty; `resetEmpty()` uses 0/0).
    /// Updated by `resetEmpty()`/`resetUnbounded()`/`resetRange()`, used by `sieveNextSegment()` to pick/clip segments.
    UInt64 range_low_odd = 3;
    UInt64 range_high_odd = MAX_UINT64;

    /// Whether current segment state is initialized and its bitmap is valid.
    /// Reset by `resetSegmentState()`, set/used by `next()`/`sieveNextSegment()`.
    bool segment_sieved = false;

    /// Segment begin value (odd). Together with `segment_odd_count` defines current segment:
    /// `[segment_begin .. segment_begin + 2*(segment_odd_count - 1)]` (inclusive).
    /// Updated by `sieveNextSegment()`, used by `next()`/`sieveSegment()`/`markCompositeInSegment()`.
    UInt64 segment_begin = 0;

    /// How many odd numbers are in the current segment.
    /// Updated by `sieveNextSegment()`, used by `next()`/`sieveSegment()`/`markCompositeInSegment()`.
    UInt64 segment_odd_count = 0;

    /// How many 64-bit words in `segment_composite_bits` contain data for current segment (`ceil(segment_odd_count / 64)`).
    /// Updated by `sieveNextSegment()`, used by `next()` and to clear/pad the bitmap.
    size_t segment_word_count = 0;

    /// Scan cursor within the current segment bitmap (odd index to check next in `next()`).
    /// Reset by `sieveNextSegment()`, advanced by `next()`.
    UInt64 next_candidate_idx = 0;

    /// Composite bitmap for the current segment; bit i corresponds to odd value `segment_begin + 2*i`.
    /// Sized to `SEGMENT_WORD_CAPACITY` to avoid reallocations; only first `segment_word_count` words are used.
    std::vector<UInt64> segment_composite_bits;

    /// Cached base odd primes used for sieving (`OddPrimesCache::ensureUpTo(sqrt(segment_end))`).
    /// Populated/extended by `sieveSegment()`, persists across segments.
    OddPrimesCache primes_cache;

    /// Number of base primes currently active for sieving; equals `next_multiple_by_prime.size()`.
    /// Reset by `resetSegmentState()`, increased by `sieveSegment()` as `sqrt(segment_end)` grows.
    size_t active_primes = 0;

    /// For each active base prime, stores the next odd multiple to mark in the current/next segment (or `NO_MULTIPLE`).
    /// Reset by `resetSegmentState()`, updated by `sieveSegment()` to resume marking efficiently across segments.
    std::vector<UInt64> next_multiple_by_prime;
};

/// Unbounded prime stream: 2, 3, 5, 7, 11, ...
class SegmentedSievePrimeGenerator
{
public:
    SegmentedSievePrimeGenerator() { sieve.resetUnbounded(); }

    UInt64 next()
    {
        if (!emitted_two)
        {
            emitted_two = true;
            return 2;
        }
        auto prime_opt = sieve.next();
        chassert(prime_opt.has_value());
        return prime_opt.value();
    }

private:
    bool emitted_two = false;
    SegmentedOddSieve sieve;
};

/// Bounded prime stream in a value range [low..high] (inclusive)
class RangeSegmentedSievePrimeGenerator
{
public:
    void setRange(UInt64 low, UInt64 high)
    {
        if (low > high)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid range for RangeSegmentedSievePrimeGenerator: [{}, {}]", low, high);

        emit_two = (low <= 2 && 2 <= high);

        if (high < 3)
        {
            sieve.resetEmpty();
            return;
        }

        /// Make low odd
        UInt64 range_low_odd = std::max<UInt64>(low, 3);
        if ((range_low_odd & 1) == 0)
            ++range_low_odd;

        /// Make high odd
        UInt64 range_high_odd = high;
        if ((range_high_odd & 1) == 0)
            --range_high_odd;

        /// [low, high] = [4, 4] -> [range_low_odd, range_high_odd] = [5, 3]
        if (range_low_odd > range_high_odd)
        {
            sieve.resetEmpty();
            return;
        }

        sieve.resetRange(range_low_odd, range_high_odd);
    }

    std::optional<UInt64> next()
    {
        if (emit_two)
        {
            emit_two = false;
            return 2;
        }

        return sieve.next();
    }

private:
    bool emit_two = false;
    SegmentedOddSieve sieve;
};

}
