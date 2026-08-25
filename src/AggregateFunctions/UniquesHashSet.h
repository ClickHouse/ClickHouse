#pragma once

#include <math.h>

#include <base/defines.h>
#include <base/types.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/Hash.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}


/** Approximate calculation of anything, as usual, is constructed according to the following scheme:
  * - some data structure is used to calculate the value of X;
  * - Not all values are added to the data structure, but only selected ones (according to some selectivity criteria);
  * - after processing all elements, the data structure is in some state S;
  * - as an approximate value of X, the value calculated according to the maximum likelihood principle is returned:
  *   at what real value X, the probability of finding the data structure in the obtained state S is maximal.
  */

/** In particular, what is described below can be found by the name of the BJKST algorithm.
  */

/** Very simple hash-set for approximate number of unique values.
  * Works like this:
  * - you can insert UInt64;
  * - before insertion, first the hash function UInt64 -> UInt32 is calculated;
  * - the original value is not saved (lost);
  * - further all operations are made with these hashes;
  * - hash table is constructed according to the scheme:
  * -  open addressing (one buffer, position in buffer is calculated by taking remainder of division by its size);
  * -  linear probing (if the cell already has a value, then the cell following it is taken, etc.);
  * -  the missing value is zero-encoded; to remember presence of zero in set, separate variable of type bool is used;
  * -  buffer growth by 2 times when filling more than 50%;
  * - if the set has more UNIQUES_HASH_MAX_SIZE elements, then all the elements are removed from the set,
  *   not divisible by 2, and then all elements that do not divide by 2 are not inserted into the set;
  * - if the situation repeats, then only elements dividing by 4, etc., are taken.
  * - the size() method returns an approximate number of elements that have been inserted into the set;
  * - there are methods for quick reading and writing in binary and text form.
  *
  * A 32-bit hash cannot distinguish more than a few billion elements: the estimate degrades quickly
  * beyond a billion, and above ~30 billion the correction formula even resulted in undefined behavior
  * (issue #6078). To keep the estimate accurate up to trillions, in addition to the main set a small
  * "wide" set of full 64-bit hashes is maintained (state version 1). It is thinned out aggressively
  * from the very beginning - only hashes with UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE zero
  * low-order bits enter it - so it stays empty or tiny for the states where the main set is accurate
  * (about 4 values per million distinct elements), and it is bounded by UNIQUES_HASH_SET_WIDE_MAX_SIZE
  * values thinned out further. Once the wide set has enough values for its own estimate to be reliable
  * (which happens at about two billion elements), size() switches to it.
  *
  * The wide set has to be maintained from the first insertion: the high bits of the hashes
  * of the already consumed elements cannot be recovered later. In particular, a state written
  * in the legacy format (state version 0) carries no wide set, and its elements were never
  * offered to one. Such a state is marked as having an incomplete wide sample: it drops its own
  * wide set, never starts a new one, and propagates the mark through merges, so that size()
  * of anything it is merged into falls back to the legacy 32-bit estimator. Estimating from
  * a wide set that covers only a part of the elements would undercount the rest entirely,
  * which is much worse than the degrading precision of the 32-bit estimate.
  */

/// The maximum degree of buffer size before the values are discarded
#define UNIQUES_HASH_MAX_SIZE_DEGREE 17

/// The maximum number of elements before the values are discarded
#define UNIQUES_HASH_MAX_SIZE (1ULL << (UNIQUES_HASH_MAX_SIZE_DEGREE - 1))

/** The number of least significant bits used for thinning. The remaining high-order bits are used to determine the position in the hash table.
  * (high-order bits are taken because the younger bits will be constant after dropping some of the values)
  */
#define UNIQUES_HASH_BITS_FOR_SKIP (32 - UNIQUES_HASH_MAX_SIZE_DEGREE)

/// Initial buffer size degree
#define UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE 4

/** Only hashes with this many zero low-order bits enter the wide set, so that it stays
  * negligibly small while the main set is accurate: one hash in 262144 passes.
  */
#define UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE 18

/// The maximum degree of the wide set buffer size before the values are thinned out further.
#define UNIQUES_HASH_SET_WIDE_MAX_SIZE_DEGREE 15

/// The maximum number of elements of the wide set: 16384 values take 256 KiB.
#define UNIQUES_HASH_SET_WIDE_MAX_SIZE (1ULL << (UNIQUES_HASH_SET_WIDE_MAX_SIZE_DEGREE - 1))

/** The estimate is taken from the wide set when it has at least this many values, i.e. starting
  * from about UNIQUES_HASH_SET_WIDE_MIN_SIZE_FOR_ESTIMATE * 2^UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE,
  * about two billion elements: at that point the statistical error of the wide estimate (about 1%)
  * meets the degrading precision of the 32-bit one.
  */
#define UNIQUES_HASH_SET_WIDE_MIN_SIZE_FOR_ESTIMATE 8192


/** This hash function is not the most optimal, but UniquesHashSet states counted with it,
  * stored in many places on disks (in many companies), so it continues to be used.
  */
struct UniquesHashSetDefaultHash
{
    size_t operator() (UInt64 x) const
    {
        return intHash32<0>(x);
    }
};


template <typename Hash = UniquesHashSetDefaultHash>
class UniquesHashSet : private HashTableAllocatorWithStackMemory<(1ULL << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt32)>
{
private:
    using Value = UInt64;
    using HashValue = UInt32;
    using Allocator = HashTableAllocatorWithStackMemory<(1ULL << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt32)>;

    /** The set of full 64-bit hashes with the same open addressing scheme as the main set.
      * It is small (it starts with much stronger thinning), so it does not need stack memory,
      * and the whole set, including this header, lives in a single lazy heap allocation.
      */
    struct WideSet
    {
        UInt32 m_size = 0;      /// Number of elements
        UInt8 size_degree = 0;  /// The size of the table as a power of 2
        UInt8 skip_degree = UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE;
        bool has_zero = false;
        UInt64 * buf = nullptr;

        size_t bufSize() const { return 1ULL << size_degree; }
        size_t maxFill() const { return 1ULL << (size_degree - 1); }
        size_t mask() const { return bufSize() - 1; }
        size_t place(UInt64 x) const { return (x >> (64 - UNIQUES_HASH_MAX_SIZE_DEGREE)) & mask(); }
        bool good(UInt64 hash) const { return hash == ((hash >> skip_degree) << skip_degree); }
    };

    /// Thinning leaves at most 2 ^ (32 - skip_degree) hashes, and `shrinkIfNeed()` raises the
    /// degree only while more than UNIQUES_HASH_MAX_SIZE of them remain, so one degree past the
    /// thinning budget is the last it can produce.
    static constexpr UInt8 max_skip_degree = UNIQUES_HASH_BITS_FOR_SKIP + 1;

    /// The analogous bound for the wide set, whose hashes are 64-bit.
    static constexpr UInt8 max_wide_skip_degree = (64 - UNIQUES_HASH_MAX_SIZE_DEGREE) + 1;

    UInt32 m_size;          /// Number of elements
    UInt8 size_degree{};      /// The size of the table as a power of 2
    UInt8 skip_degree;      /// Skip elements not divisible by 2 ^ skip_degree
    bool has_zero;          /// The hash table contains an element with a hash value of 0.

    HashValue * buf{};

    /// nullptr while no hash has passed the wide thinning (i.e. for most of the states).
    WideSet * wide = nullptr;

    /** Some of the elements of this set were never offered to the wide set, so a wide estimate
      * would miss them. This happens when a state serialized in the legacy format (state version 0)
      * is read or merged in. The mark is sticky: the missing high bits cannot be recovered.
      */
    bool wide_incomplete = false;

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
    /// For profiling.
    mutable size_t collisions;
#endif

    void alloc(UInt8 new_size_degree)
    {
        buf = reinterpret_cast<HashValue *>(Allocator::alloc((1ULL << new_size_degree) * sizeof(buf[0])));
        size_degree = new_size_degree;
    }

    void free()
    {
        if (buf)
        {
            Allocator::free(buf, buf_size() * sizeof(buf[0]));
            buf = nullptr;
        }
    }

    size_t buf_size() const           { return 1ULL << size_degree; } /// NOLINT
    size_t max_fill() const           { return 1ULL << (size_degree - 1); } /// NOLINT
    size_t mask() const               { return buf_size() - 1; }

    size_t place(HashValue x) const { return (x >> UNIQUES_HASH_BITS_FOR_SKIP) & mask(); }

    /// The value is divided by 2 ^ skip_degree
    bool good(HashValue hash) const { return hash == ((hash >> skip_degree) << skip_degree); }

    static UInt64 fullHash(Value key) { return static_cast<UInt64>(Hash()(key)); }

    HashValue hash(Value key) const { return static_cast<HashValue>(Hash()(key)); }

    /// Delete all values whose hashes do not divide by 2 ^ skip_degree
    void rehash()
    {
        for (size_t i = 0; i < buf_size(); ++i)
        {
            if (buf[i])
            {
                if (!good(buf[i]))
                {
                    buf[i] = 0;
                    --m_size;
                }
                /** After removing the elements, there may have been room for items,
                  * which were placed further than necessary, due to a collision.
                  * You need to move them.
                  */
                else if (i != place(buf[i]))
                {
                    HashValue x = buf[i];
                    buf[i] = 0;
                    reinsertImpl(x);
                }
            }
        }

        /** We must process first collision resolution chain once again.
          * Look at the comment in "resize" function.
          */
        for (size_t i = 0; i < buf_size() && buf[i]; ++i)
        {
            if (i != place(buf[i]))
            {
                HashValue x = buf[i];
                buf[i] = 0;
                reinsertImpl(x);
            }
        }
    }

    /// Increase the size of the buffer 2 times or up to new_size_degree, if it is non-zero.
    void resize(size_t new_size_degree = 0)
    {
        size_t old_size = buf_size();

        if (!new_size_degree)
            new_size_degree = size_degree + 1;

        /// Expand the space.
        buf = reinterpret_cast<HashValue *>(Allocator::realloc(buf, old_size * sizeof(buf[0]), (1ULL << new_size_degree) * sizeof(buf[0])));
        size_degree = static_cast<UInt8>(new_size_degree);

        /** Now some items may need to be moved to a new location.
          * The element can stay in place, or move to a new location "on the right",
          * or move to the left of the collision resolution chain, because the elements to the left of it have been moved to the new "right" location.
          * There is also a special case
          *    if the element was to be at the end of the old buffer,                        [        x]
          *    but is at the beginning because of the collision resolution chain,            [o       x]
          *    then after resizing, it will first be out of place again,                     [        xo        ]
          *    and in order to transfer it to where you need it,
          *    will have to be after transferring all elements from the old half             [         o   x    ]
          *    process another tail from the collision resolution chain immediately after it [        o    x    ]
          * This is why || buf[i] below.
          */
        for (size_t i = 0; i < old_size || buf[i]; ++i)
        {
            HashValue x = buf[i];
            if (!x)
                continue;

            size_t place_value = place(x);

            /// The element is in its place.
            if (place_value == i)
                continue;

            while (buf[place_value] && buf[place_value] != x)
            {
                ++place_value;
                place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
                ++collisions;
#endif
            }

            /// The element remained in its place.
            if (buf[place_value] == x)
                continue;

            buf[place_value] = x;
            buf[i] = 0;
        }
    }

    /// Insert a value.
    void insertImpl(HashValue x)
    {
        if (x == 0)
        {
            m_size += !has_zero;
            has_zero = true;
            return;
        }

        size_t place_value = place(x);
        while (buf[place_value] && buf[place_value] != x)
        {
            ++place_value;
            place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        if (buf[place_value] == x)
            return;

        buf[place_value] = x;
        ++m_size;
    }

    /** Insert a value into the new buffer that was in the old buffer.
      * Used when increasing the size of the buffer, as well as when reading from a file.
      */
    void reinsertImpl(HashValue x)
    {
        size_t place_value = place(x);
        while (buf[place_value])
        {
            ++place_value;
            place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        buf[place_value] = x;
    }

    /** If the hash table is full enough, then do resize.
      * If there are too many items, then throw half the pieces until they are small enough.
      */
    void shrinkIfNeed()
    {
        if (unlikely(m_size > max_fill()))
        {
            if (m_size > UNIQUES_HASH_MAX_SIZE)
            {
                while (m_size > UNIQUES_HASH_MAX_SIZE)
                {
                    if (unlikely(skip_degree >= max_skip_degree))
                        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                            "Cannot thin out UniquesHashSet: {} elements remain at the maximum skip degree of {}",
                            static_cast<size_t>(m_size), static_cast<size_t>(max_skip_degree));

                    ++skip_degree;
                    rehash();
                }
            }
            else
                resize();
        }
    }

    UInt8 wideSkipDegree() const { return wide ? wide->skip_degree : UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE; }

    static WideSet * allocWideSet(UInt8 new_size_degree)
    {
        /// The header and the table are placed in a single allocation.
        size_t bytes = sizeof(WideSet) + (1ULL << new_size_degree) * sizeof(UInt64);
        WideSet * res = new (HashTableAllocator().alloc(bytes)) WideSet;
        res->size_degree = new_size_degree;
        res->buf = reinterpret_cast<UInt64 *>(reinterpret_cast<char *>(res) + sizeof(WideSet));
        return res;
    }

    static void freeWideSet(WideSet * set)
    {
        if (set)
            HashTableAllocator().free(set, sizeof(WideSet) + set->bufSize() * sizeof(UInt64));
    }

    void freeWide()
    {
        freeWideSet(wide);
        wide = nullptr;
    }

    /// The wide set no longer represents all the elements - drop it and never start a new one.
    void markWideIncomplete()
    {
        wide_incomplete = true;
        freeWide();
    }

    /// Copy the wide set with a table of the given size (the values are reinserted, not memcpy'd).
    static WideSet * resizeWideSet(const WideSet * old, UInt8 new_size_degree)
    {
        WideSet * res = allocWideSet(new_size_degree);
        res->m_size = old->m_size;
        res->skip_degree = old->skip_degree;
        res->has_zero = old->has_zero;
        for (size_t i = 0; i < old->bufSize(); ++i)
            if (old->buf[i])
                wideReinsertImpl(*res, old->buf[i]);
        return res;
    }

    static void wideReinsertImpl(WideSet & set, UInt64 x)
    {
        size_t place_value = set.place(x);
        while (set.buf[place_value])
        {
            ++place_value;
            place_value &= set.mask();
        }
        set.buf[place_value] = x;
    }

    /// Delete all values of the wide set whose hashes do not divide by 2 ^ skip_degree.
    static void wideRehash(WideSet & set)
    {
        for (size_t i = 0; i < set.bufSize(); ++i)
        {
            if (set.buf[i])
            {
                if (!set.good(set.buf[i]))
                {
                    set.buf[i] = 0;
                    --set.m_size;
                }
                else if (i != set.place(set.buf[i]))
                {
                    UInt64 x = set.buf[i];
                    set.buf[i] = 0;
                    wideReinsertImpl(set, x);
                }
            }
        }

        for (size_t i = 0; i < set.bufSize() && set.buf[i]; ++i)
        {
            if (i != set.place(set.buf[i]))
            {
                UInt64 x = set.buf[i];
                set.buf[i] = 0;
                wideReinsertImpl(set, x);
            }
        }
    }

    void wideInsert(UInt64 x)
    {
        if (unlikely(wide_incomplete))
            return;

        /// The caller may have checked the value against a stale (smaller) thinning mask
        /// for the sake of the hot loop, so the check is repeated here.
        if (x & ((1ULL << wideSkipDegree()) - 1))
            return;

        if (!wide)
            wide = allocWideSet(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);

        if (x == 0)
        {
            wide->m_size += !wide->has_zero;
            wide->has_zero = true;
            return;
        }

        size_t place_value = wide->place(x);
        while (wide->buf[place_value] && wide->buf[place_value] != x)
        {
            ++place_value;
            place_value &= wide->mask();
        }

        if (wide->buf[place_value] == x)
            return;

        wide->buf[place_value] = x;
        ++wide->m_size;

        if (unlikely(wide->m_size > wide->maxFill()))
        {
            if (wide->m_size > UNIQUES_HASH_SET_WIDE_MAX_SIZE)
            {
                while (wide->m_size > UNIQUES_HASH_SET_WIDE_MAX_SIZE)
                {
                    if (unlikely(wide->skip_degree >= max_wide_skip_degree))
                        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                            "Cannot thin out the wide set of UniquesHashSet: {} elements remain at the maximum skip degree of {}",
                            static_cast<size_t>(wide->m_size), static_cast<size_t>(max_wide_skip_degree));

                    ++wide->skip_degree;
                    wideRehash(*wide);
                }
            }
            else
            {
                WideSet * new_set = resizeWideSet(wide, wide->size_degree + 1);
                freeWide();
                wide = new_set;
            }
        }
    }

    UInt64 wideSkipMask() const { return (1ULL << wideSkipDegree()) - 1; }

    /** Wide candidates have at least UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE zero low-order bits,
      * and the thinning of the main set never exceeds 16 bits (at the degree of 16 the number
      * of possible sampled 32-bit hashes is already down to the maximum size of the set),
      * so every wide candidate passes good() of the main set, and it is enough to check
      * the candidates on that rare path - the hot loops stay free of extra work.
      * The check is a pre-filter on the truncated hash: it passes a superset of the candidates
      * (an exact one while the wide thinning fits 32 bits), and `wideInsert` checks the full hash
      * against the current mask itself - which also makes a stale pre-filter mask harmless.
      */
    HashValue widePrefilterMask() const { return static_cast<HashValue>(wideSkipMask()); }

    /** With how many different elements, when randomly scattered across 2^32 buckets,
      * the given number of non-empty buckets is obtained on average.
      * This is the correction of the systematic error due to collisions of the 32-bit hashes.
      */
    static size_t correctFor32BitCollisions(size_t res)
    {
        size_t p32 = 1ULL << 32;

        /** When the number of distinct elements is much larger than 2^32, almost all buckets are filled
          * and `res` may reach (or, due to the pseudo-random remainder, exceed) 2^32,
          * where the formula below is not defined (previously this resulted in undefined behavior
          * when casting infinity or NaN to an integer). Saturate the estimate instead:
          * 32 bits of a hash contain no information about larger cardinalities anyway
          * (and the wide set takes over long before this point).
          */
        if (res >= p32)
            res = p32 - 1;

        return static_cast<size_t>(round(static_cast<double>(p32) * (log(static_cast<double>(p32)) - log(static_cast<double>(p32 - res)))));
    }


public:
    using value_type = Value;

    UniquesHashSet() : // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - base class is an allocator with stack memory, initialized in alloc()
        m_size(0),
        skip_degree(0),
        has_zero(false)
    {
        alloc(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
        collisions = 0;
#endif
    }

    UniquesHashSet(const UniquesHashSet & rhs) // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - base class is an allocator with stack memory, initialized in alloc()
        : m_size(rhs.m_size), skip_degree(rhs.skip_degree), has_zero(rhs.has_zero), wide_incomplete(rhs.wide_incomplete)
    {
        alloc(rhs.size_degree);
        memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));

        if (rhs.wide)
        {
            wide = allocWideSet(rhs.wide->size_degree);
            wide->m_size = rhs.wide->m_size;
            wide->skip_degree = rhs.wide->skip_degree;
            wide->has_zero = rhs.wide->has_zero;
            memcpy(wide->buf, rhs.wide->buf, rhs.wide->bufSize() * sizeof(UInt64));
        }
    }

    UniquesHashSet & operator=(const UniquesHashSet & rhs)
    {
        if (&rhs == this)
            return *this;

        if (size_degree != rhs.size_degree)
        {
            free();
            alloc(rhs.size_degree);
        }

        m_size = rhs.m_size;
        skip_degree = rhs.skip_degree;
        has_zero = rhs.has_zero;
        wide_incomplete = rhs.wide_incomplete;

        memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));

        freeWide();
        if (rhs.wide)
        {
            wide = allocWideSet(rhs.wide->size_degree);
            wide->m_size = rhs.wide->m_size;
            wide->skip_degree = rhs.wide->skip_degree;
            wide->has_zero = rhs.wide->has_zero;
            memcpy(wide->buf, rhs.wide->buf, rhs.wide->bufSize() * sizeof(UInt64));
        }

        return *this;
    }

    ~UniquesHashSet()
    {
        free();
        freeWide();
    }

    void ALWAYS_INLINE insert(Value x)
    {
        const HashValue hash_value = hash(x);
        if (!good(hash_value))
            return;

        if (unlikely((hash_value & widePrefilterMask()) == 0))
            wideInsert(fullHash(x));

        insertImpl(hash_value);
        shrinkIfNeed();
    }

    /// This value is arbitrary. The optimal value might depend on the CPU pipeline depth, cache line size (64B = 8 UInt64s),
    /// hash collision rate, instruction parallelism, etc.
    /// We choose a value that is big enough to provide sufficient instruction level parallelism but not too big to bloat the code size.
    static constexpr size_t insert_many_batch_size = 8;

    template <typename SourceType, auto Transform>
    requires std::is_invocable_r_v<Value, decltype(Transform), const SourceType &>
    void insertMany(const SourceType * data, size_t size)
    {
        /** The candidates for the wide set (see the comment at `widePrefilterMask`) are only detected
          * in the main loop - branchlessly, on the truncated hashes - and are inserted after it:
          * a call inside the loop would make the compiler cautiously reload the fields of the set
          * from memory all the time, slowing the loop down by tens of percent.
          * A candidate is expected once per 262144 values, so the positions are collected
          * into a small buffer; on its overflow (pathological input) the tail after the last
          * recorded position is simply rescanned - insertion into the wide set is idempotent.
          */
        std::array<UInt32, 16> wide_candidate_positions; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - only the first num_wide_candidates entries are read
        size_t num_wide_candidates = 0;

        const HashValue wide_prefilter = widePrefilterMask();

        size_t i = 0;
        while (i < size)
        {
            /// Check that we have enough capacity and enough values to insert in batch. If not, we will insert values one by one which
            /// allows us to check the shrink condition after each insert and avoid inserting too many values before shrinking.
            if ((max_fill() - m_size) >= insert_many_batch_size && ((size - i) >= insert_many_batch_size))
            {
                /// We read and transform multiple values at once which allows both the compiler and the CPU to better optimize the code.
                /// We calculate place() even for !good() hashes to maximize data independence and enable better out-of-order execution.
                /// The extra work is negligible compared to the instruction level parallelization benefits.
                std::array<HashValue, insert_many_batch_size> hash_value; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - filled by the loop below before read
                size_t wide_candidates = 0;
                for (size_t j = 0; j < insert_many_batch_size; ++j)
                {
                    hash_value[j] = hash(Transform(data[i + j]));
                    wide_candidates += !(hash_value[j] & wide_prefilter);
                }

                if (unlikely(wide_candidates))
                {
                    for (size_t j = 0; j < insert_many_batch_size; ++j)
                        if ((hash_value[j] & wide_prefilter) == 0 && num_wide_candidates < wide_candidate_positions.size())
                            wide_candidate_positions[num_wide_candidates++] = static_cast<UInt32>(i + j);
                }

                i += insert_many_batch_size;

                std::array<size_t, insert_many_batch_size> place_value_batch; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - filled by the loop below before read
                for (size_t j = 0; j < insert_many_batch_size; ++j)
                {
                    place_value_batch[j] = place(hash_value[j]);
                }

                for (size_t j = 0; j < insert_many_batch_size; ++j)
                {
                    const HashValue & x = hash_value[j];
                    if (!good(x))
                        continue;

                    if (x == 0)
                    {
                        m_size += !has_zero;
                        has_zero = true;
                        continue;
                    }

                    size_t place_value = place_value_batch[j];
                    while (buf[place_value] && buf[place_value] != x)
                    {
                        ++place_value;
                        place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
                        ++collisions;
#endif
                    }

                    if (buf[place_value] == x)
                        continue;

                    buf[place_value] = x;
                    ++m_size;
                }
            }
            else
            {
                const HashValue hash_value = hash(Transform(data[i]));

                if (unlikely((hash_value & wide_prefilter) == 0) && num_wide_candidates < wide_candidate_positions.size())
                    wide_candidate_positions[num_wide_candidates++] = static_cast<UInt32>(i);

                i++;
                if (!good(hash_value))
                    continue;

                insertImpl(hash_value);
            }

            /// We need to check shrink condition after each batch or single insert
            shrinkIfNeed();
        }

        if (unlikely(num_wide_candidates))
        {
            for (size_t k = 0; k < num_wide_candidates; ++k)
                wideInsert(fullHash(Transform(data[wide_candidate_positions[k]])));

            /// The buffer was full: rescan the tail after the last recorded position.
            if (num_wide_candidates == wide_candidate_positions.size())
            {
                for (size_t k = wide_candidate_positions.back() + 1; k < size; ++k)
                {
                    const UInt64 full_hash = fullHash(Transform(data[k]));
                    if (unlikely((full_hash & wideSkipMask()) == 0))
                        wideInsert(full_hash);
                }
            }
        }
    }

    size_t size() const
    {
        /** The wide set gives a reliable estimate once it has enough values;
          * below that the 32-bit main set is more precise. If the wide sample does not cover
          * all the elements (see `wide_incomplete`), it is not there at all and the legacy
          * estimator is used, because a partial wide estimate would silently lose the rest.
          */
        if (wide && wide->m_size >= UNIQUES_HASH_SET_WIDE_MIN_SIZE_FOR_ESTIMATE)
        {
            size_t res = static_cast<size_t>(wide->m_size) * (1ULL << wide->skip_degree);

            /** Pseudo-random remainder - in order to be not visible,
              * that the number is divided by the power of two.
              */
            res += (intHashCRC32(wide->m_size) & ((1ULL << wide->skip_degree) - 1));

            /** Collisions of 64-bit hashes are negligible: their systematic error would reach
              * just 0.003% at 10^15 distinct elements, far below the statistical error of the estimate.
              */
            return res;
        }

        if (0 == skip_degree)
            return m_size;

        size_t res = m_size * (1ULL << skip_degree);

        /** Pseudo-random remainder - in order to be not visible,
          * that the number is divided by the power of two.
          */
        res += (intHashCRC32(m_size) & ((1ULL << skip_degree) - 1));

        /** Correction of a systematic error due to collisions during hashing in UInt32.
          * `fixed_res(res)` formula
          * - with how many different elements of fixed_res,
          *   when randomly scattered across 2^32 buckets,
          *   filled buckets with average of res is obtained.
          */
        return correctFor32BitCollisions(res);
    }

    /// For tests.
    size_t wideSetSize() const { return wide ? wide->m_size : 0; }
    bool isWideIncomplete() const { return wide_incomplete; }

    void merge(const UniquesHashSet & rhs)
    {
        /// The elements of `rhs` that are missing from its wide set are missing from ours as well.
        if (rhs.wide_incomplete)
            markWideIncomplete();

        if (rhs.skip_degree > skip_degree)
        {
            skip_degree = rhs.skip_degree;
            rehash();
        }

        if (!has_zero && rhs.has_zero)
        {
            has_zero = true;
            ++m_size;
            shrinkIfNeed();
        }

        for (size_t i = 0; i < rhs.buf_size(); ++i)
        {
            if (rhs.buf[i] && good(rhs.buf[i]))
            {
                insertImpl(rhs.buf[i]);
                shrinkIfNeed();
            }
        }

        /// An incomplete state must not carry a wide set at all (see `markWideIncomplete`), so the
        /// elements of `rhs.wide` are not merged - otherwise an empty wide set would be allocated
        /// here (`wideInsert` refuses to fill it) and the state would serialize with both flag bits
        /// set, which the format forbids.
        if (rhs.wide && !wide_incomplete)
        {
            if (rhs.wide->skip_degree > wideSkipDegree())
            {
                if (!wide)
                    wide = allocWideSet(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
                wide->skip_degree = rhs.wide->skip_degree;
                wideRehash(*wide);
            }

            if (rhs.wide->has_zero && (!wide || !wide->has_zero))
                wideInsert(0);

            for (size_t i = 0; i < rhs.wide->bufSize(); ++i)
            {
                UInt64 x = rhs.wide->buf[i];
                if (x && (x & ((1ULL << wideSkipDegree()) - 1)) == 0)
                    wideInsert(x);
            }
        }
    }

    /** Two serialization formats exist.
      * The legacy format (state version 0): skip_degree, then the number of values, then the 32-bit values
      * of the main set.
      * The new format (state version 1): a flags byte (bit 0 - a wide set follows the main set,
      * bit 1 - the wide sample is incomplete, see `wide_incomplete`; the two are mutually exclusive),
      * then the main set in the legacy layout, then, if present, the wide set: its skip_degree,
      * the number of values, and the 64-bit values.
      */
    void write(DB::WriteBuffer & wb, bool use_legacy_format) const
    {
        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot write UniquesHashSet: too large size_degree.");

        /// A null `buf` here would indicate upstream state corruption (e.g. a double-destroyed state).
        chassert(buf);

        if (!use_legacy_format)
        {
            UInt8 flags = (wide ? 1 : 0) | (wide_incomplete ? 2 : 0);
            DB::writeBinaryLittleEndian(flags, wb);
        }

        DB::writeBinaryLittleEndian(skip_degree, wb);
        DB::writeVarUInt(m_size, wb);

        if (has_zero)
        {
            HashValue x = 0;
            DB::writeBinaryLittleEndian(x, wb);
        }

        for (size_t i = 0; i < buf_size(); ++i)
            if (buf[i])
                DB::writeBinaryLittleEndian(buf[i], wb);

        if (use_legacy_format || !wide)
            return;

        DB::writeBinaryLittleEndian(wide->skip_degree, wb);
        DB::writeVarUInt(wide->m_size, wb);

        if (wide->has_zero)
        {
            UInt64 x = 0;
            DB::writeBinaryLittleEndian(x, wb);
        }

        for (size_t i = 0; i < wide->bufSize(); ++i)
            if (wide->buf[i])
                DB::writeBinaryLittleEndian(wide->buf[i], wb);
    }

    void read(DB::ReadBuffer & rb, bool use_legacy_format)
    {
        has_zero = false;
        wide_incomplete = false;
        freeWide();

        bool has_wide = false;
        if (!use_legacy_format)
        {
            UInt8 flags = 0;
            DB::readBinaryLittleEndian(flags, rb);
            if ((flags & ~3) || (flags & 3) == 3)
                throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                    "Cannot read UniquesHashSet: invalid flags {}: the defined bits are mutually exclusive and no other bit is allowed",
                    static_cast<size_t>(flags));
            has_wide = flags & 1;
            wide_incomplete = flags & 2;
        }

        DB::readBinaryLittleEndian(skip_degree, rb);

        if (unlikely(skip_degree > max_skip_degree))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                "Cannot read UniquesHashSet: skip degree is {}, which exceeds the maximum value of {}",
                static_cast<size_t>(skip_degree), static_cast<size_t>(max_skip_degree));

        DB::readVarUInt(m_size, rb);

        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        /** A non-empty state written in the legacy format carries elements that were never offered
          * to a wide set, so no wide estimate of this state (or of anything it is merged into)
          * can be trusted. An empty legacy state contributes nothing and does not spoil a merge.
          */
        if (use_legacy_format && m_size > 0)
            wide_incomplete = true;

        free();

        UInt8 new_size_degree = m_size <= 1
            ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
            : static_cast<UInt8>(std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2));

        alloc(new_size_degree);

        if (m_size <= 1)
        {
            for (size_t i = 0; i < m_size; ++i)
            {
                HashValue x = 0;
                DB::readBinaryLittleEndian(x, rb);
                if (x == 0)
                    has_zero = true;
                else
                    reinsertImpl(x);
            }
        }
        else
        {
            auto hs = std::make_unique<HashValue[]>(m_size);
            rb.readStrict(reinterpret_cast<char *>(hs.get()), m_size * sizeof(HashValue));

            for (size_t i = 0; i < m_size; ++i)
            {
                DB::transformEndianness<std::endian::native, std::endian::little>(hs[i]);
                if (hs[i] == 0)
                    has_zero = true;
                else
                    reinsertImpl(hs[i]);
            }
        }

        if (!has_wide)
            return;

        UInt8 wide_skip_degree = 0;
        DB::readBinaryLittleEndian(wide_skip_degree, rb);
        size_t wide_size = 0;
        DB::readVarUInt(wide_size, rb);

        if (unlikely(wide_size > UNIQUES_HASH_SET_WIDE_MAX_SIZE))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                "Cannot read UniquesHashSet: the wide set size is {}, which exceeds the maximum value of {}",
                wide_size, static_cast<size_t>(UNIQUES_HASH_SET_WIDE_MAX_SIZE));

        if (unlikely(wide_skip_degree > max_wide_skip_degree))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                "Cannot read UniquesHashSet: the wide set skip degree is {}, which exceeds the maximum value of {}",
                static_cast<size_t>(wide_skip_degree), static_cast<size_t>(max_wide_skip_degree));

        UInt8 wide_size_degree = wide_size <= 1
            ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
            : static_cast<UInt8>(std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(wide_size - 1)) + 2));

        wide = allocWideSet(wide_size_degree);
        wide->m_size = static_cast<UInt32>(wide_size);
        wide->skip_degree = wide_skip_degree;

        for (size_t i = 0; i < wide_size; ++i)
        {
            UInt64 x = 0;
            DB::readBinaryLittleEndian(x, rb);
            if (x == 0)
                wide->has_zero = true;
            else
                wideReinsertImpl(*wide, x);
        }
    }

    static void skip(DB::ReadBuffer & rb, bool use_legacy_format)
    {
        size_t size = 0;

        bool has_wide = false;
        if (!use_legacy_format)
        {
            UInt8 flags = 0;
            DB::readBinaryLittleEndian(flags, rb);
            if ((flags & ~3) || (flags & 3) == 3)
                throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                    "Cannot read UniquesHashSet: invalid flags {}: the defined bits are mutually exclusive and no other bit is allowed",
                    static_cast<size_t>(flags));
            has_wide = flags & 1;
        }

        rb.ignore();
        DB::readVarUInt(size, rb);

        if (size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        rb.ignore(sizeof(HashValue) * size);

        if (!has_wide)
            return;

        rb.ignore();
        DB::readVarUInt(size, rb);

        if (unlikely(size > UNIQUES_HASH_SET_WIDE_MAX_SIZE))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                "Cannot read UniquesHashSet: the wide set size is {}, which exceeds the maximum value of {}",
                size, static_cast<size_t>(UNIQUES_HASH_SET_WIDE_MAX_SIZE));

        rb.ignore(sizeof(UInt64) * size);
    }

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
    size_t getCollisions() const
    {
        return collisions;
    }
#endif
};


#undef UNIQUES_HASH_MAX_SIZE_DEGREE
#undef UNIQUES_HASH_MAX_SIZE
#undef UNIQUES_HASH_BITS_FOR_SKIP
#undef UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
#undef UNIQUES_HASH_SET_WIDE_INITIAL_SKIP_DEGREE
#undef UNIQUES_HASH_SET_WIDE_MAX_SIZE_DEGREE
#undef UNIQUES_HASH_SET_WIDE_MAX_SIZE
#undef UNIQUES_HASH_SET_WIDE_MIN_SIZE_FOR_ESTIMATE
