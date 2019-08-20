#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTable.h>

#define CASE_1_8 \
    case 1: \
    case 2: \
    case 3: \
    case 4: \
    case 5: \
    case 6: \
    case 7: \
    case 8

#define CASE_9_16 \
    case 9: \
    case 10: \
    case 11: \
    case 12: \
    case 13: \
    case 14: \
    case 15: \
    case 16

#define CASE_17_24 \
    case 17: \
    case 18: \
    case 19: \
    case 20: \
    case 21: \
    case 22: \
    case 23: \
    case 24

struct StringKey0
{
};

using StringKey8 = UInt64;
using StringKey16 = DB::UInt128;
struct StringKey24
{
    UInt64 a;
    UInt64 b;
    UInt64 c;

    bool operator==(const StringKey24 rhs) const { return a == rhs.a && b == rhs.b && c == rhs.c; }
    bool operator!=(const StringKey24 rhs) const { return !operator==(rhs); }
    bool operator==(const UInt64 rhs) const { return a == rhs && b == 0 && c == 0; }
    bool operator!=(const UInt64 rhs) const { return !operator==(rhs); }

    StringKey24 & operator=(const UInt64 rhs)
    {
        a = rhs;
        b = 0;
        c = 0;
        return *this;
    }
};

inline StringRef ALWAYS_INLINE toStringRef(const StringKey8 & n)
{
    return {reinterpret_cast<const char *>(&n), 8ul - (__builtin_clzll(n) >> 3)};
}
inline StringRef ALWAYS_INLINE toStringRef(const StringKey16 & n)
{
    return {reinterpret_cast<const char *>(&n), 16ul - (__builtin_clzll(n.high) >> 3)};
}
inline StringRef ALWAYS_INLINE toStringRef(const StringKey24 & n)
{
    return {reinterpret_cast<const char *>(&n), 24ul - (__builtin_clzll(n.c) >> 3)};
}
inline const StringRef & ALWAYS_INLINE toStringRef(const StringRef & s)
{
    return s;
}

struct StringHashTableHash
{
#if defined(__SSE4_2__)
    size_t ALWAYS_INLINE operator()(StringKey8 key) const
    {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key);
        return res;
    }
    size_t ALWAYS_INLINE operator()(StringKey16 key) const
    {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key.low);
        res = _mm_crc32_u64(res, key.high);
        return res;
    }
    size_t ALWAYS_INLINE operator()(StringKey24 key) const
    {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key.a);
        res = _mm_crc32_u64(res, key.b);
        res = _mm_crc32_u64(res, key.c);
        return res;
    }
#else
    size_t ALWAYS_INLINE operator()(StringKey8 key) const
    {
        return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&key), 8);
    }
    size_t ALWAYS_INLINE operator()(StringKey16 key) const
    {
        return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&key), 16);
    }
    size_t ALWAYS_INLINE operator()(StringKey24 key) const
    {
        return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&key), 24);
    }
#endif
    size_t ALWAYS_INLINE operator()(StringRef key) const
    {
        return StringRefHash()(key);
    }
};

template <typename Cell>
struct StringHashTableEmpty
{
    using Self = StringHashTableEmpty;

    bool has_zero = false;
    std::aligned_storage_t<sizeof(Cell), alignof(Cell)> zero_value_storage; /// Storage of element with zero key.

public:
    bool hasZero() const { return has_zero; }

    void setHasZero()
    {
        has_zero = true;
        new (zeroValue()) Cell();
    }

    void setHasZero(const Cell & other)
    {
        has_zero = true;
        new (zeroValue()) Cell(other);
    }

    void clearHasZero()
    {
        has_zero = false;
        if (!std::is_trivially_destructible_v<Cell>)
            zeroValue()->~Cell();
    }

    Cell * zeroValue() { return reinterpret_cast<Cell *>(&zero_value_storage); }
    const Cell * zeroValue() const { return reinterpret_cast<const Cell *>(&zero_value_storage); }

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder &&, LookupResult & it, bool & inserted, size_t /* hash */)
    {
        if (!hasZero())
        {
            setHasZero();
            inserted = true;
        }
        else
            inserted = false;
        it = zeroValue();
    }

    template <typename Key>
    LookupResult ALWAYS_INLINE find(Key, size_t /* hash */)
    {
        return hasZero() ? zeroValue() : nullptr;
    }


    void write(DB::WriteBuffer & wb) const { zeroValue()->write(wb); }
    void writeText(DB::WriteBuffer & wb) const { zeroValue()->writeText(wb); }
    void read(DB::ReadBuffer & rb) { zeroValue()->read(rb); }
    void readText(DB::ReadBuffer & rb) { zeroValue()->readText(rb); }
    size_t size() const { return hasZero() ? 1 : 0; }
    bool empty() const { return !hasZero(); }
    size_t getBufferSizeInBytes() const { return sizeof(Cell); }
    size_t getCollisions() const { return 0; }
};

template <size_t initial_size_degree = 8>
struct StringHashTableGrower : public HashTableGrower<initial_size_degree>
{
    // Smooth growing for string maps
    void increaseSize() { this->size_degree += 1; }
};

template <typename SubMaps>
class StringHashTable : private boost::noncopyable
{
protected:
    static constexpr size_t NUM_MAPS = 5;
    // Map for storing empty string
    using T0 = typename SubMaps::T0;

    // Short strings are stored as numbers
    using T1 = typename SubMaps::T1;
    using T2 = typename SubMaps::T2;
    using T3 = typename SubMaps::T3;

    // Long strings are stored as StringRef along with saved hash
    using Ts = typename SubMaps::Ts;
    using Self = StringHashTable;

    template <typename, typename, size_t>
    friend class TwoLevelStringHashTable;

    T0 m0;
    T1 m1;
    T2 m2;
    T3 m3;
    Ts ms;

public:
    using Key = StringRef;
    using key_type = Key;
    using value_type = typename Ts::value_type;
    using LookupResult = typename Ts::mapped_type *;

    StringHashTable() {}

    StringHashTable(size_t reserve_for_num_elements)
        : m1{reserve_for_num_elements / 4}
        , m2{reserve_for_num_elements / 4}
        , m3{reserve_for_num_elements / 4}
        , ms{reserve_for_num_elements / 4}
    {
    }

    StringHashTable(StringHashTable && rhs) { *this = std::move(rhs); }
    ~StringHashTable() {}

public:
    // Dispatch is written in a way that maximizes the performance:
    // 1. Always memcpy 8 times bytes
    // 2. Use switch case extension to generate fast dispatching table
    // 3. Combine hash computation along with key loading
    // 4. Funcs are named callables that can be force_inlined
    // NOTE: It relies on Little Endianness and SSE4.2
    template <typename KeyHolder, typename Func>
    decltype(auto) ALWAYS_INLINE dispatch(KeyHolder && key_holder, Func && func)
    {
        static constexpr StringKey0 key0{};
        const StringRef & x = keyHolderGetKey(key_holder);
        size_t sz = x.size;
        const char * p = x.data;
        // pending bits that needs to be shifted out
        char s = (-sz & 7) * 8;
        union
        {
            StringKey8 k8;
            StringKey16 k16;
            StringKey24 k24;
            UInt64 n[3];
        };
        StringHashTableHash hash;
        switch (sz)
        {
            case 0:
                keyHolderDiscardKey(key_holder);
                return func(m0, key0, 0);
            CASE_1_8 : {
                // first half page
                if ((reinterpret_cast<uintptr_t>(p) & 2048) == 0)
                {
                    memcpy(&n[0], p, 8);
                    n[0] &= -1ul >> s;
                }
                else
                {
                    const char * lp = x.data + x.size - 8;
                    memcpy(&n[0], lp, 8);
                    n[0] >>= s;
                }
                keyHolderDiscardKey(key_holder);
                return func(m1, k8, hash(k8));
            }
            CASE_9_16 : {
                memcpy(&n[0], p, 8);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[1], lp, 8);
                n[1] >>= s;
                keyHolderDiscardKey(key_holder);
                return func(m2, k16, hash(k16));
            }
            CASE_17_24 : {
                memcpy(&n[0], p, 16);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[2], lp, 8);
                n[2] >>= s;
                keyHolderDiscardKey(key_holder);
                return func(m3, k24, hash(k24));
            }
            default: {
                return func(ms, std::forward<KeyHolder>(key_holder), hash(x));
            }
        }
    }

    struct EmplaceCallable
    {
        LookupResult & mapped;
        bool & inserted;

        EmplaceCallable(LookupResult & mapped_, bool & inserted_)
            : mapped(mapped_), inserted(inserted_) {}

        template <typename Map, typename KeyHolder>
        void ALWAYS_INLINE operator()(Map & map, KeyHolder && key_holder, size_t hash)
        {
            typename Map::LookupResult result;
            map.emplace(key_holder, result, inserted, hash);
            mapped = lookupResultGetMapped(result);
        }
    };

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        this->dispatch(key_holder, EmplaceCallable(it, inserted));
    }

    struct FindCallable
    {
        template <typename Map, typename KeyHolder>
        LookupResult ALWAYS_INLINE operator()(Map & map, KeyHolder && key_holder, size_t hash)
        {
            return lookupResultGetMapped(map.find(keyHolderGetKey(key_holder), hash));
        }
    };

    LookupResult ALWAYS_INLINE find(Key x)
    {
        return dispatch(x, FindCallable{});
    }

    void write(DB::WriteBuffer & wb) const
    {
        m0.write(wb);
        m1.write(wb);
        m2.write(wb);
        m3.write(wb);
        ms.write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        m0.writeText(wb);
        DB::writeChar(',', wb);
        m1.writeText(wb);
        DB::writeChar(',', wb);
        m2.writeText(wb);
        DB::writeChar(',', wb);
        m3.writeText(wb);
        DB::writeChar(',', wb);
        ms.writeText(wb);
    }

    void read(DB::ReadBuffer & rb)
    {
        m0.read(rb);
        m1.read(rb);
        m2.read(rb);
        m3.read(rb);
        ms.read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        m0.readText(rb);
        DB::assertChar(',', rb);
        m1.readText(rb);
        DB::assertChar(',', rb);
        m2.readText(rb);
        DB::assertChar(',', rb);
        m3.readText(rb);
        DB::assertChar(',', rb);
        ms.readText(rb);
    }

    size_t size() const { return m0.size() + m1.size() + m2.size() + m3.size() + ms.size(); }

    bool empty() const { return m0.empty() && m1.empty() && m2.empty() && m3.empty() && ms.empty(); }

    size_t getBufferSizeInBytes() const
    {
        return m0.getBufferSizeInBytes() + m1.getBufferSizeInBytes() + m2.getBufferSizeInBytes() + m3.getBufferSizeInBytes()
            + ms.getBufferSizeInBytes();
    }

    void clearAndShrink()
    {
        m1.clearHasZero();
        m1.clearAndShrink();
        m2.clearAndShrink();
        m3.clearAndShrink();
        ms.clearAndShrink();
    }
};
