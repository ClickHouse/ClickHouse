#pragma once
#include <base/StringRef.h>
#include <Common/HashTable/Hash.h>

namespace DB
{

template <typename T>
struct StringRefWithFixedKey
{
    T fixed{};
    StringRef ref;
};

template <typename T>
inline bool operator== (const StringRefWithFixedKey<T> & lhs, const StringRefWithFixedKey<T> & rhs)
{
    return lhs.fixed == rhs.fixed && lhs.ref == rhs.ref;
}

template <typename T>
struct StringRefWithFixedKeyHash64
{
    size_t operator() (const StringRefWithFixedKey<T> & x) const
    {
        size_t hash_fixed = HashCRC32<T>()(x.fixed);
        size_t hash_ref = StringRefHash64()(x.ref);
        return CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash_fixed, hash_ref));
    }
};

#if defined(CRC_INT)

template <typename T>
struct StringRefWithFixedKeyCRC32
{
    unsigned operator() (const StringRefWithFixedKey<T> & x) const
    {
        unsigned hash_fixed = static_cast<unsigned>(HashCRC32<T>()(x.fixed));
        unsigned hash_ref = CRC32Hash()(x.ref);
        return static_cast<unsigned>(CRC_INT(hash_fixed, hash_ref));
    }
};

template <typename T>
struct StringRefWithFixedKeyHash : StringRefWithFixedKeyCRC32<T> {};

#else

template <typename T>
struct StringRefWithFixedKeyCRC32
{
    unsigned operator() (const StringRefWithFixedKey<T> & x) const
    {
       throw std::logic_error{"Not implemented CRC32Hash without SSE"};
    }
};

template <typename T>
struct StringRefWithFixedKeyHash : StringRefWithFixedKeyHash64<T> {};

#endif

}

namespace ZeroTraits
{
    template <typename T> bool check(const DB::StringRefWithFixedKey<T> & x)
    {
        return 0 == x.ref.size && x.fixed == T{};
    }

    template <typename T> void set(DB::StringRefWithFixedKey<T> & x)
    {
        x.ref.size = 0;
        x.fixed = T{};
    }
}
