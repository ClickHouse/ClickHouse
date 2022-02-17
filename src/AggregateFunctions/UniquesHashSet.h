#pragma once

#include <cstdint>
#include <math.h>

#include <base/types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableAllocator.h>


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


/** This hash function is not the most optimal, but UniquesHashSet states counted with it,
  * stored in many places on disks (in the Yandex.Metrika), so it continues to be used.
  */
struct UniquesHashSetDefaultHash
{
    size_t operator()(UInt64 x) const { return intHash32<0>(x); }
};


#define ADDR_HAS_TYPE(NAME) \
    template <typename, typename = void> \
    struct addr_has_type_##NAME : std::false_type \
    { \
    }; \
    template <typename T> \
    struct addr_has_type_##NAME<T, std::void_t<typename T::NAME>> : std::true_type \
    { \
    };

ADDR_HAS_TYPE(Buf);
ADDR_HAS_TYPE(Value);
template <class T>
concept UniqueHashAddressable
    = addr_has_type_Buf<T>::value && addr_has_type_Value<T>::value && std::is_pointer<typename T::Buf>::value && requires(
        typename T::Buf buf, size_t start_index, size_t max_index, typename T::Value x, size_t mask)
{
    {
        T::place(x, mask)
        } -> std::same_as<size_t>;
    {
        T::addressingForResize(buf, start_index, max_index, x)
        } -> std::same_as<size_t>;
    {
        T::addressingForInsert(buf, start_index, max_index, x)
        } -> std::same_as<size_t>;
    {
        T::addressingForReInsert(buf, start_index, max_index)
        } -> std::same_as<size_t>;
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
    {
        T::setCollisions(nullptr)
    }
#endif
};


template <typename HashValue>
class HashAddressingForNoneSIMD
{
public:
    using Buf = HashValue *;
    using Value = HashValue;
    static uint64_t * pcollisions;
    inline static size_t place(HashValue x, size_t mask) { return (x >> UNIQUES_HASH_BITS_FOR_SKIP) & mask; }
    inline static size_t addressingForResize(Buf buf, size_t start_index, size_t max_index, HashValue x)
    {
        size_t place_value = start_index;
        while (buf[place_value] && buf[place_value] != x)
        {
            ++place_value;
            place_value &= max_index;

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            ++(*pcollisions);
#endif
        }
        return place_value;
    }
    inline static size_t addressingForInsert(Buf buf, size_t start_index, size_t max_index, HashValue x)
    {
        size_t place_value = start_index;
        while (buf[place_value] && buf[place_value] != x)
        {
            ++place_value;
            place_value &= max_index;

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            ++(*pcollisions);
#endif
        }
        return place_value;
    }
    inline static size_t addressingForReInsert(Buf buf, size_t start_index, size_t max_index)
    {
        size_t place_value = start_index;
        while (buf[place_value])
        {
            ++place_value;
            place_value &= max_index;

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            ++(*pcollisions);
#endif
        }
        return place_value;
    }
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
    static void setCollisions(uint64_t * p) { pcollisions = p; }
#endif
};

#if defined(__SSE2__)
template <typename HashValue>
class HashAddressingForSSE2
{
private:
    inline static const __m128i zero16 = _mm_setzero_si128();
    /**For sse2 processing, we should process 4 continues number
    * in one shot. And for current buffer resize aglo, it can
    * keep our buffer size always modular by 4. So if start index
    * can modular by 4, it will always satisfy sse2 processing in
    * hash addressing. If start index can not modular 4, we should
    * start with aligned start index to make processing more simple
    * and more efficiency. When start index can not modular 4, we still
    * use aligned start index to calc address. But some test case
    * (eg 01890_state_of_state) need our data distribute the same as
    * original algo, this made us ignore some part that ahead start index.
    * Or the data's distribution will different with original aglo.
    * This will cause some fast test case fail
    **/
    inline static size_t alignedStart(size_t start_index) { return (start_index & 0xFFFFFFFFFFFFFFFC); }
    inline static UInt16 calcInitIgnoreMask(size_t start_index, size_t aligned_start)
    {
        if (start_index == aligned_start)
            return 0xffff;
        auto offset_sse2 = ((start_index - aligned_start) << 2);
        //clear lower offset bits
        return (0xFFFF >> offset_sse2) << offset_sse2;
    }

public:
    using Buf = HashValue *;
    using Value = HashValue;
    static uint64_t * pcollisions;

    inline static size_t place(HashValue x, size_t mask) { return (x >> UNIQUES_HASH_BITS_FOR_SKIP) & mask; }

    inline static size_t addressingForResize(Buf buf, size_t start_index, size_t max_index, HashValue x)
    {
        if (0 == buf[start_index] || x == buf[start_index])
        {
            return start_index;
        }
        size_t place_value = start_index;
        __m128i x4s = _mm_set_epi32(x, x, x, x);
        size_t aligned_start = alignedStart(start_index);
        UInt16 ignore_mask = calcInitIgnoreMask(start_index, aligned_start);
        start_index = aligned_start;
        bool is_first = true;
        while (true)
        {
            start_index &= max_index;
            HashValue * cstart_addr = buf + start_index;
            __m128i c128v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(cstart_addr));
            if (!is_first)
            {
                ignore_mask = (static_cast<UInt16>(0xFFFF));
            }
            is_first = false;
            UInt16 cmask = ((_mm_movemask_epi8(_mm_cmpeq_epi32(c128v, x4s) | _mm_cmpeq_epi32(c128v, zero16))));
            cmask &= ignore_mask;
            if (cmask)
            {
                size_t offset = __builtin_ctz(cmask);
                place_value = start_index + (offset >> 2);
#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
                *pcollisions += (offset >> 2) - 1;
#    endif
                break;
            }
            start_index += 4;
#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            *pcollisions += 4;
#    endif
        }
        return place_value;
    }

    inline static size_t addressingForInsert(Buf buf, size_t start_index, size_t max_index, HashValue x)
    {
        if (0 == buf[start_index] || x == buf[start_index])
        {
            return start_index;
        }
        auto place_value = start_index;
        __m128i x4s = _mm_set_epi32(x, x, x, x);
        size_t aligned_start = alignedStart(start_index);
        UInt16 ignore_mask = calcInitIgnoreMask(start_index, aligned_start);
        start_index = aligned_start;
        bool is_first = true;
        while (true)
        {
            start_index &= max_index;
            HashValue * cstart_addr = buf + start_index;
            __m128i c128v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(cstart_addr));
            if (!is_first)
            {
                ignore_mask = (static_cast<UInt16>(0xFFFF));
            }

            is_first = false;
            UInt16 cmask = ((_mm_movemask_epi8(_mm_cmpeq_epi32(c128v, x4s) | _mm_cmpeq_epi32(c128v, zero16))));
            cmask &= ignore_mask;
            if (cmask)
            {
                size_t offset = __builtin_ctz(cmask);
                place_value = start_index + (offset >> 2);
#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
                collisions += (offset >> 2) - 1;
#    endif
                break;
            }
            start_index += 4;
#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            *pcollisions += 4;
#    endif
        }
        return place_value;
    }

    inline static size_t addressingForReInsert(Buf buf, size_t start_index, size_t max_index)
    {
        if (0 == buf[start_index])
        {
            return start_index;
        }
        auto place_value = start_index;
        size_t aligned_start = alignedStart(start_index);
        UInt16 ignore_mask = calcInitIgnoreMask(start_index, aligned_start);
        start_index = aligned_start;
        bool is_first = true;
        while (true)
        {
            start_index &= max_index;
            HashValue * cstart_addr = buf + start_index;
            __m128i c128v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(cstart_addr));

            if (!is_first)
            {
                ignore_mask = (static_cast<UInt16>(0xFFFF));
            }
            is_first = false;
            UInt16 cmask = ((_mm_movemask_epi8(_mm_cmpeq_epi32(c128v, zero16))) & ignore_mask);

            if (cmask)
            {
                size_t offset = __builtin_ctz(cmask);
                place_value = start_index + (offset >> 2);
#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
                *pcollisions += (offset >> 2) - 1;
#    endif
                break;
            }
            start_index += 4;
#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
            *pcollisions += 4;
#    endif
        }
        return place_value;
    }

#    ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
    static void setCollisions(uint64_t * p) { pcollisions = p; }
#    endif
};
#endif

template <typename Hash = UniquesHashSetDefaultHash, UniqueHashAddressable AddrPolicy = HashAddressingForNoneSIMD<UInt32>>
class UniquesHashSetBase : private HashTableAllocatorWithStackMemory<(1ULL << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt32)>
{
private:
    using Value = UInt64;
    using HashValue = UInt32;
    using Allocator = HashTableAllocatorWithStackMemory<(1ULL << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt32)>;

    UInt32 m_size; /// Number of elements
    UInt8 size_degree; /// The size of the table as a power of 2
    UInt8 skip_degree; /// Skip elements not divisible by 2 ^ skip_degree
    bool has_zero; /// The hash table contains an element with a hash value of 0.

    HashValue * buf;

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
            Allocator::free(buf, bufSize() * sizeof(buf[0]));
            buf = nullptr;
        }
    }

    inline size_t bufSize() const { return 1ULL << size_degree; }
    inline size_t maxFill() const { return 1ULL << (size_degree - 1); }
    inline size_t mask() const { return bufSize() - 1; }
    inline size_t place(HashValue x) const { return AddrPolicy::place(x, mask()); }

    /// The value is divided by 2 ^ skip_degree
    inline bool good(HashValue hash) const { return hash == ((hash >> skip_degree) << skip_degree); }

    HashValue hash(Value key) const { return Hash()(key); }

    /// Delete all values whose hashes do not divide by 2 ^ skip_degree
    void rehash()
    {
        for (size_t i = 0; i < bufSize(); ++i)
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
        for (size_t i = 0; i < bufSize() && buf[i]; ++i)
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
        size_t old_size = bufSize();

        if (!new_size_degree)
            new_size_degree = size_degree + 1;

        /// Expand the space.
        buf = reinterpret_cast<HashValue *>(Allocator::realloc(buf, old_size * sizeof(buf[0]), (1ULL << new_size_degree) * sizeof(buf[0])));
        size_degree = new_size_degree;

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
            place_value = AddrPolicy::addressingForResize(buf, place_value, mask(), x);
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
        place_value = AddrPolicy::addressingForInsert(buf, place_value, mask(), x);
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
        place_value = AddrPolicy::addressingForReInsert(buf, place_value, mask());
        buf[place_value] = x;
    }

    /** If the hash table is full enough, then do resize.
      * If there are too many items, then throw half the pieces until they are small enough.
      */
    void shrinkIfNeed()
    {
        if (unlikely(m_size > maxFill()))
        {
            if (m_size > UNIQUES_HASH_MAX_SIZE)
            {
                while (m_size > UNIQUES_HASH_MAX_SIZE)
                {
                    ++skip_degree;
                    rehash();
                }
            }
            else
                resize();
        }
    }


public:
    using value_type = Value;

    UniquesHashSetBase() : m_size(0), skip_degree(0), has_zero(false)
    {
        alloc(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
        collisions = 0;
        AddrPolicy::setCollisions(&collisions);
#endif
    }

    UniquesHashSetBase(const UniquesHashSetBase & rhs) : m_size(rhs.m_size), skip_degree(rhs.skip_degree), has_zero(rhs.has_zero)
    {
        alloc(rhs.size_degree);
        memcpy(buf, rhs.buf, bufSize() * sizeof(buf[0]));
    }

    UniquesHashSetBase & operator=(const UniquesHashSetBase & rhs)
    {
        if (this == &rhs)
            return *this;
        if (size_degree != rhs.size_degree)
        {
            free();
            alloc(rhs.size_degree);
        }

        m_size = rhs.m_size;
        skip_degree = rhs.skip_degree;
        has_zero = rhs.has_zero;

        memcpy(buf, rhs.buf, bufSize() * sizeof(buf[0]));

        return *this;
    }

    ~UniquesHashSetBase() { free(); }

    void insert(Value x)
    {
        HashValue hash_value = hash(x);
        if (!good(hash_value))
            return;

        insertImpl(hash_value);
        shrinkIfNeed();
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

        /** Correction of a systematic error due to collisions during hashing in UInt32.
          * `fixed_res(res)` formula
          * - with how many different elements of fixed_res,
          *   when randomly scattered across 2^32 buckets,
          *   filled buckets with average of res is obtained.
          */
        size_t p32 = 1ULL << 32;
        size_t fixed_res = round(p32 * (log(p32) - log(p32 - res)));
        return fixed_res;
    }

    void merge(const UniquesHashSetBase & rhs)
    {
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

        for (size_t i = 0; i < rhs.bufSize(); ++i)
        {
            if (rhs.buf[i] && good(rhs.buf[i]))
            {
                insertImpl(rhs.buf[i]);
                shrinkIfNeed();
            }
        }
    }

    void write(DB::WriteBuffer & wb) const
    {
        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot write UniquesHashSet: too large size_degree.");

        DB::writeIntBinary(skip_degree, wb);
        DB::writeVarUInt(m_size, wb);

        if (has_zero)
        {
            HashValue x = 0;
            DB::writeIntBinary(x, wb);
        }

        for (size_t i = 0; i < bufSize(); ++i)
            if (buf[i])
                DB::writeIntBinary(buf[i], wb);
    }

    void read(DB::ReadBuffer & rb)
    {
        has_zero = false;

        DB::readIntBinary(skip_degree, rb);
        DB::readVarUInt(m_size, rb);

        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        free();

        UInt8 new_size_degree = m_size <= 1 ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
                                            : std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2);

        alloc(new_size_degree);

        for (size_t i = 0; i < m_size; ++i)
        {
            HashValue x = 0;
            DB::readIntBinary(x, rb);
            if (x == 0)
                has_zero = true;
            else
                reinsertImpl(x);
        }
    }

    void readAndMerge(DB::ReadBuffer & rb)
    {
        UInt8 rhs_skip_degree = 0;
        DB::readIntBinary(rhs_skip_degree, rb);

        if (rhs_skip_degree > skip_degree)
        {
            skip_degree = rhs_skip_degree;
            rehash();
        }

        size_t rhs_size = 0;
        DB::readVarUInt(rhs_size, rb);

        if (rhs_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        if ((1ULL << size_degree) < rhs_size)
        {
            UInt8 new_size_degree = std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(rhs_size - 1)) + 2);
            resize(new_size_degree);
        }

        for (size_t i = 0; i < rhs_size; ++i)
        {
            HashValue x = 0;
            DB::readIntBinary(x, rb);
            insertHash(x);
        }
    }

    static void skip(DB::ReadBuffer & rb)
    {
        size_t size = 0;

        rb.ignore();
        DB::readVarUInt(size, rb);

        if (size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        rb.ignore(sizeof(HashValue) * size);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot write UniquesHashSet: too large size_degree.");

        DB::writeIntText(skip_degree, wb);
        wb.write(",", 1);
        DB::writeIntText(m_size, wb);

        if (has_zero)
            wb.write(",0", 2);

        for (size_t i = 0; i < bufSize(); ++i)
        {
            if (buf[i])
            {
                wb.write(",", 1);
                DB::writeIntText(buf[i], wb);
            }
        }
    }

    void readText(DB::ReadBuffer & rb)
    {
        has_zero = false;

        DB::readIntText(skip_degree, rb);
        DB::assertChar(',', rb);
        DB::readIntText(m_size, rb);

        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        free();

        UInt8 new_size_degree = m_size <= 1 ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
                                            : std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2);

        alloc(new_size_degree);

        for (size_t i = 0; i < m_size; ++i)
        {
            HashValue x = 0;
            DB::assertChar(',', rb);
            DB::readIntText(x, rb);
            if (x == 0)
                has_zero = true;
            else
                reinsertImpl(x);
        }
    }

    void insertHash(HashValue hash_value)
    {
        if (!good(hash_value))
            return;

        insertImpl(hash_value);
        shrinkIfNeed();
    }

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
    size_t getCollisions() const { return collisions; }
#endif
};


template <typename Hash = UniquesHashSetDefaultHash>
#if defined(__SSE2__)
using UniquesHashSet = UniquesHashSetBase<Hash, HashAddressingForSSE2<UInt32>>;
#else
using UniquesHashSet = UniquesHashSetBase<Hash, HashAddressingForNoneSIMD<UInt32>>;
#endif

#undef UNIQUES_HASH_MAX_SIZE_DEGREE
#undef UNIQUES_HASH_MAX_SIZE
#undef UNIQUES_HASH_BITS_FOR_SKIP
#undef UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
