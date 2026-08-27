#pragma once

#include <math.h>

#include <base/types.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/Hash.h>


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

    UInt32 m_size;          /// Number of elements
    UInt8 size_degree;      /// The size of the table as a power of 2
    UInt8 skip_degree;      /// Skip elements not divisible by 2 ^ skip_degree
    bool has_zero;          /// The hash table contains an element with a hash value of 0.

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

    UniquesHashSet() :
        m_size(0),
        skip_degree(0),
        has_zero(false)
    {
        alloc(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
        collisions = 0;
#endif
    }

    UniquesHashSet(const UniquesHashSet & rhs)
        : m_size(rhs.m_size), skip_degree(rhs.skip_degree), has_zero(rhs.has_zero)
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

        memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));

        return *this;
    }

    ~UniquesHashSet()
    {
        free();
    }

    void ALWAYS_INLINE insert(Value x)
    {
        const HashValue hash_value = hash(x);
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
        size_t fixed_res = static_cast<size_t>(round(p32 * (log(p32) - log(p32 - res))));
        return fixed_res;
    }

    void merge(const UniquesHashSet & rhs)
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

        for (size_t i = 0; i < rhs.buf_size(); ++i)
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
    }

    void read(DB::ReadBuffer & rb)
    {
        has_zero = false;

        DB::readBinaryLittleEndian(skip_degree, rb);
        DB::readVarUInt(m_size, rb);

        if (m_size > UNIQUES_HASH_MAX_SIZE)
            throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

        free();

        UInt8 new_size_degree = m_size <= 1
             ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
             : std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2);

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

        for (size_t i = 0; i < buf_size(); ++i)
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

        UInt8 new_size_degree = m_size <= 1
             ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
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
