#pragma once

#include <math.h>

#include <base/defines.h>
#include <base/types.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>


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
  * - before insertion, first the hash function UInt64 -> UInt64 is calculated;
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
  * Compatibility note. Historically the hashes were truncated to 32 bits, which capped the number
  * of distinguishable elements at around ten billion: for larger cardinalities the estimate
  * saturated and even resulted in undefined behavior in the correction formula (issue #6078).
  * Now the full 64-bit hash is kept. States written in the legacy format (by old servers,
  * or read from tables created before the change) identify elements by only the low 32 bits
  * of the hash, and the discarded bits cannot be recovered. Such states are kept in a special
  * "compat" mode: the stored values carry only 32 bits of information, and merging a 64-bit state
  * with a compat state downgrades the former by truncating its hashes - the result is exactly
  * the same as if the whole calculation had been done with 32-bit hashes. The same truncation
  * is applied when a state is serialized for an old server.
  *
  * In the compat mode, a 32-bit hash h is stored in the 64-bit cell as (h << 32) | h.
  * With this encoding both halves of the cell equal h, so the bits used for the position
  * in the hash table (the high-order ones) and the bits used for thinning (the low-order ones)
  * coincide with what the legacy 32-bit implementation used, and no separate code path
  * is needed for lookups and thinning.
  */

/// The maximum degree of buffer size before the values are discarded
#define UNIQUES_HASH_MAX_SIZE_DEGREE 17

/// The maximum number of elements before the values are discarded
#define UNIQUES_HASH_MAX_SIZE (1ULL << (UNIQUES_HASH_MAX_SIZE_DEGREE - 1))

/** The number of least significant bits used for thinning. The remaining high-order bits are used to determine the position in the hash table.
  * (high-order bits are taken because the younger bits will be constant after dropping some of the values)
  */
#define UNIQUES_HASH_BITS_FOR_SKIP (64 - UNIQUES_HASH_MAX_SIZE_DEGREE)

/// Initial buffer size degree
#define UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE 4


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
class UniquesHashSet : private HashTableAllocatorWithStackMemory<(1ULL << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt64)>
{
private:
    using Value = UInt64;
    using HashValue = UInt64;
    using Allocator = HashTableAllocatorWithStackMemory<(1ULL << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt64)>;

    /// Thinning leaves at most 2 ^ (32 - skip_degree) hashes, and `shrinkIfNeed()` raises the
    /// degree only while more than UNIQUES_HASH_MAX_SIZE of them remain, so one degree past the
    /// thinning budget is the last it can produce.
    static constexpr UInt8 max_skip_degree = UNIQUES_HASH_BITS_FOR_SKIP + 1;

    UInt32 m_size;          /// Number of elements
    UInt8 size_degree{};      /// The size of the table as a power of 2
    UInt8 skip_degree;      /// Skip elements not divisible by 2 ^ skip_degree
    bool has_zero;          /// The hash table contains an element with a hash value of 0.
    bool compat_32bit;      /// The stored values carry only 32 bits of hash information (see the comment above).

    HashValue * buf{};

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

    HashValue hash(Value key) const { return static_cast<HashValue>(Hash()(key)); }

    /// The representation of a 32-bit hash in the compat mode (see the comment above).
    static HashValue replicate32(HashValue x)
    {
        HashValue low = x & 0xFFFFFFFFULL;
        return (low << 32) | low;
    }

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

    template <typename StoredValue>
    void readImpl(DB::ReadBuffer & rb)
    {
        if (m_size <= 1)
        {
            for (size_t i = 0; i < m_size; ++i)
            {
                StoredValue x = 0;
                DB::readBinaryLittleEndian(x, rb);
                insertValueRead(static_cast<HashValue>(x));
            }
        }
        else
        {
            auto hs = std::make_unique<StoredValue[]>(m_size);
            rb.readStrict(reinterpret_cast<char *>(hs.get()), m_size * sizeof(StoredValue));

            for (size_t i = 0; i < m_size; ++i)
            {
                DB::transformEndianness<std::endian::native, std::endian::little>(hs[i]);
                insertValueRead(static_cast<HashValue>(hs[i]));
            }
        }
    }

    void insertValueRead(HashValue x)
    {
        if (compat_32bit)
            x = replicate32(x);

        if (x == 0)
            has_zero = true;
        else
            reinsertImpl(x);
    }


public:
    using value_type = Value;

    UniquesHashSet() : // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - base class is an allocator with stack memory, initialized in alloc()
        m_size(0),
        skip_degree(0),
        has_zero(false),
        compat_32bit(false)
    {
        alloc(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
        collisions = 0;
#endif
    }

    UniquesHashSet(const UniquesHashSet & rhs) // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - base class is an allocator with stack memory, initialized in alloc()
        : m_size(rhs.m_size), skip_degree(rhs.skip_degree), has_zero(rhs.has_zero), compat_32bit(rhs.compat_32bit)
    {
        alloc(rhs.size_degree);
        memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));
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
        compat_32bit = rhs.compat_32bit;

        memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));

        return *this;
    }

    ~UniquesHashSet()
    {
        free();
    }

    void ALWAYS_INLINE insert(Value x)
    {
        HashValue hash_value = hash(x);
        if (unlikely(compat_32bit))
            hash_value = replicate32(hash_value);

        if (!good(hash_value))
            return;

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
        /// States downgraded to the compat mode are rare and not worth a separate batch implementation.
        if (unlikely(compat_32bit))
        {
            for (size_t i = 0; i < size; ++i)
                insert(Transform(data[i]));
            return;
        }

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
                for (size_t j = 0; j < insert_many_batch_size; ++j)
                {
                    hash_value[j] = hash(Transform(data[i + j]));
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
                i++;
                if (!good(hash_value))
                    continue;

                insertImpl(hash_value);
            }

            /// We need to check shrink condition after each batch or single insert
            shrinkIfNeed();
        }
    }

    size_t size() const
    {
        if (0 == skip_degree)
            return m_size;

        size_t res = m_size * (1ULL << skip_degree);

        /** Pseudo-random remainder - in order to be not visible,
          * that the number is divided by the power of two.
          */
        res += (intHashCRC32(m_size) & ((1ULL << skip_degree) - 1));

        /** With 64-bit hashes, the systematic error due to hash collisions is negligible:
          * it reaches just 0.003% at 10^15 distinct elements,
          * which is far below the statistical error of the estimate itself.
          */
        if (!compat_32bit)
            return res;

        size_t p32 = 1ULL << 32;

        /// The correction below is undefined once the thinned count reaches the hash space:
        /// report the largest count it can express.
        if (unlikely(res >= p32))
            res = p32 - 1;

        /** Correction of a systematic error due to collisions during hashing in UInt32.
          * `fixed_res(res)` formula
          * - with how many different elements of fixed_res,
          *   when randomly scattered across 2^32 buckets,
          *   filled buckets with average of res is obtained.
          */
        size_t fixed_res = static_cast<size_t>(round(static_cast<double>(p32) * (log(p32) - log(p32 - res))));
        return fixed_res;
    }

    /** Convert the state to the compat mode, where elements are identified by only the low
      * 32 bits of the hash (as done by the legacy implementation). The stored values are truncated;
      * values that collide after truncation are deduplicated exactly as the legacy implementation would do.
      * This loses information and cannot be undone.
      */
    void downgradeToCompat32()
    {
        if (compat_32bit)
            return;

        compat_32bit = true;

        if (0 == m_size)
            return;

        /// Truncated values are placed by different bits of the hash, so the table is rebuilt from scratch.
        DB::PODArray<HashValue> values;
        values.reserve(m_size);
        for (size_t i = 0; i < buf_size(); ++i)
            if (buf[i])
                values.push_back(replicate32(buf[i]));

        memset(buf, 0, buf_size() * sizeof(buf[0]));
        m_size = has_zero ? 1 : 0;

        for (HashValue x : values)
        {
            /// A value with zero low 32 bits truncates to the zero hash.
            if (x == 0)
            {
                m_size += !has_zero;
                has_zero = true;
            }
            else
                insertImpl(x);
        }
    }

    bool isCompat32() const { return compat_32bit; }

    void merge(const UniquesHashSet & rhs)
    {
        /** If the other state was built (fully or partially) with 32-bit hashes, there is no way
          * to recover the missing bits of its hashes. Downgrade this state to the compat mode
          * to keep deduplication correct: the result is the same as if the whole calculation
          * had been done with 32-bit hashes.
          */
        if (rhs.compat_32bit && !compat_32bit)
            downgradeToCompat32();

        const bool need_truncation = compat_32bit && !rhs.compat_32bit;

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
            if (!rhs.buf[i])
                continue;

            HashValue x = need_truncation ? replicate32(rhs.buf[i]) : rhs.buf[i];

            if (x == 0)
            {
                /// A truncated value with zero low 32 bits becomes the zero hash.
                if (!has_zero)
                {
                    has_zero = true;
                    ++m_size;
                    shrinkIfNeed();
                }
            }
            else if (good(x))
            {
                insertImpl(x);
                shrinkIfNeed();
            }
        }
    }

    /** Two serialization formats exist.
      * The legacy format (state version 0): skip_degree, then the number of values, then the values truncated to 32 bits.
      * The new format (state version 1): a flags byte (bit 0 - the values carry only 32 bits of hash information),
      * then skip_degree, then the number of values, then the values, 4 or 8 bytes each according to the flag.
      */
    void write(DB::WriteBuffer & wb, bool use_legacy_format) const
    {
        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot write UniquesHashSet: too large size_degree.");

        /// A null `buf` here would indicate upstream state corruption (e.g. a double-destroyed state).
        chassert(buf);

        if (use_legacy_format && !compat_32bit)
        {
            /// Serialization for an old server. The hashes have to be truncated to 32 bits;
            /// the truncated values are exactly what an old server would have computed for the same elements.
            UniquesHashSet compat_copy(*this);
            compat_copy.downgradeToCompat32();
            compat_copy.write(wb, /*use_legacy_format=*/ true);
            return;
        }

        if (!use_legacy_format)
        {
            UInt8 flags = compat_32bit ? 1 : 0;
            DB::writeBinaryLittleEndian(flags, wb);
        }

        DB::writeBinaryLittleEndian(skip_degree, wb);
        DB::writeVarUInt(m_size, wb);

        if (has_zero)
        {
            if (compat_32bit)
                DB::writeBinaryLittleEndian(static_cast<UInt32>(0), wb);
            else
                DB::writeBinaryLittleEndian(static_cast<UInt64>(0), wb);
        }

        for (size_t i = 0; i < buf_size(); ++i)
        {
            if (buf[i])
            {
                /// In the compat mode both halves of the stored value equal the 32-bit hash.
                if (compat_32bit)
                    DB::writeBinaryLittleEndian(static_cast<UInt32>(buf[i]), wb);
                else
                    DB::writeBinaryLittleEndian(buf[i], wb);
            }
        }
    }

    void read(DB::ReadBuffer & rb, bool use_legacy_format)
    {
        has_zero = false;

        if (use_legacy_format)
        {
            compat_32bit = true;
        }
        else
        {
            UInt8 flags = 0;
            DB::readBinaryLittleEndian(flags, rb);
            if (flags & ~1)
                throw Poco::Exception("Cannot read UniquesHashSet: unknown flags.");
            compat_32bit = flags & 1;
        }

        DB::readBinaryLittleEndian(skip_degree, rb);

        if (unlikely(skip_degree > max_skip_degree))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA,
                "Cannot read UniquesHashSet: skip degree is {}, which exceeds the maximum value of {}",
                static_cast<size_t>(skip_degree), static_cast<size_t>(max_skip_degree));

        DB::readVarUInt(m_size, rb);

        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        /// The structure never produces skip_degree above 48 (at that point the number of possible
        /// values passing the thinning is already below the maximum number of stored elements),
        /// and shifting by 64 or more would be undefined behavior.
        if (skip_degree >= 64)
            throw Poco::Exception("Cannot read UniquesHashSet: too large skip_degree.");

        free();

        UInt8 new_size_degree = m_size <= 1
            ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
            : static_cast<UInt8>(std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2));

        alloc(new_size_degree);

        if (compat_32bit)
            readImpl<UInt32>(rb);
        else
            readImpl<UInt64>(rb);
    }

    static void skip(DB::ReadBuffer & rb, bool use_legacy_format)
    {
        size_t size = 0;

        bool values_32bit = true;
        if (!use_legacy_format)
        {
            UInt8 flags = 0;
            DB::readBinaryLittleEndian(flags, rb);
            if (flags & ~1)
                throw Poco::Exception("Cannot read UniquesHashSet: unknown flags.");
            values_32bit = flags & 1;
        }

        rb.ignore();
        DB::readVarUInt(size, rb);

        if (size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        rb.ignore((values_32bit ? sizeof(UInt32) : sizeof(UInt64)) * size);
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
