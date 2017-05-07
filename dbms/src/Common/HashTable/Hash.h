#pragma once

#include <Core/Types.h>


/** Hash functions that are better than the trivial function std::hash.
  * (when aggregated by the visitor ID, the performance increase is more than 5 times)
  */

/** Taken from MurmurHash.
  * Faster than intHash32 when inserting into the hash table UInt64 -> UInt64, where the key is the visitor ID.
  */
inline DB::UInt64 intHash64(DB::UInt64 x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

/** CRC32C is not very high-quality as a hash function,
  *  according to avalanche and bit independence tests, as well as a small number of bits,
  *  but can behave well when used in hash tables,
  *  due to high speed (latency 3 + 1 clock cycle, throughput 1 clock cycle).
  * Works only with SSE 4.2 support.
  * Used asm instead of intrinsics, so you do not have to build the entire project with -msse4.
  */
inline DB::UInt64 intHashCRC32(DB::UInt64 x)
{
#if defined(__x86_64__)
    DB::UInt64 crc = -1ULL;
    asm("crc32q %[x], %[crc]\n" : [crc] "+r" (crc) : [x] "rm" (x));
    return crc;
#else
    /// On other platforms we do not need CRC32. NOTE This can be confusing.
    return intHash64(x);
#endif
}


template <typename T> struct DefaultHash;

template <typename T>
inline size_t DefaultHash64(T key)
{
    union
    {
        T in;
        DB::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHash64(u.out);
}

#define DEFINE_HASH(T) \
template <> struct DefaultHash<T>\
{\
    size_t operator() (T key) const\
    {\
        return DefaultHash64<T>(key);\
    }\
};

DEFINE_HASH(DB::UInt8)
DEFINE_HASH(DB::UInt16)
DEFINE_HASH(DB::UInt32)
DEFINE_HASH(DB::UInt64)
DEFINE_HASH(DB::Int8)
DEFINE_HASH(DB::Int16)
DEFINE_HASH(DB::Int32)
DEFINE_HASH(DB::Int64)
DEFINE_HASH(DB::Float32)
DEFINE_HASH(DB::Float64)

#undef DEFINE_HASH


template <typename T> struct HashCRC32;

template <typename T>
inline size_t hashCRC32(T key)
{
    union
    {
        T in;
        DB::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHashCRC32(u.out);
}

#define DEFINE_HASH(T) \
template <> struct HashCRC32<T>\
{\
    size_t operator() (T key) const\
    {\
        return hashCRC32<T>(key);\
    }\
};

DEFINE_HASH(DB::UInt8)
DEFINE_HASH(DB::UInt16)
DEFINE_HASH(DB::UInt32)
DEFINE_HASH(DB::UInt64)
DEFINE_HASH(DB::Int8)
DEFINE_HASH(DB::Int16)
DEFINE_HASH(DB::Int32)
DEFINE_HASH(DB::Int64)
DEFINE_HASH(DB::Float32)
DEFINE_HASH(DB::Float64)

#undef DEFINE_HASH


/// It is reasonable to use for UInt8, UInt16 with sufficient hash table size.
struct TrivialHash
{
    template <typename T>
    size_t operator() (T key) const
    {
        return key;
    }
};


/** A relatively good non-cryptic hash function from UInt64 to UInt32.
  * But worse (both in quality and speed) than just cutting intHash64.
  * Taken from here: http://www.concentric.net/~ttwang/tech/inthash.htm
  *
  * Slightly changed compared to the function by link: shifts to the right are accidentally replaced by a cyclic shift to the right.
  * This change did not affect the smhasher test results.
  *
  * It is recommended to use different salt for different tasks.
  * That was the case that in the database values ​​were sorted by hash (for low-quality pseudo-random spread),
  *  and in another place, in the aggregate function, the same hash was used in the hash table,
  *  as a result, this aggregate function was monstrously slowed due to collisions.
  */
template <DB::UInt64 salt>
inline DB::UInt32 intHash32(DB::UInt64 key)
{
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return key;
}


/// For containers.
template <typename T, DB::UInt64 salt = 0>
struct IntHash32
{
    size_t operator() (const T & key) const
    {
        return intHash32<salt>(key);
    }
};
