#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTable.h>

#include <new>
#include <variant>


using StringKey8 = UInt64;
using StringKey16 = DB::UInt128;
struct StringKey24
{
    UInt64 a;
    UInt64 b;
    UInt64 c;

    bool operator==(const StringKey24 rhs) const { return a == rhs.a && b == rhs.b && c == rhs.c; }
};

inline StringRef ALWAYS_INLINE toStringRef(const StringKey8 & n)
{
    assert(n != 0);
    return {reinterpret_cast<const char *>(&n), 8ul - (__builtin_clzll(n) >> 3)};
}
inline StringRef ALWAYS_INLINE toStringRef(const StringKey16 & n)
{
    assert(n.items[1] != 0);
    return {reinterpret_cast<const char *>(&n), 16ul - (__builtin_clzll(n.items[1]) >> 3)};
}
inline StringRef ALWAYS_INLINE toStringRef(const StringKey24 & n)
{
    assert(n.c != 0);
    return {reinterpret_cast<const char *>(&n), 24ul - (__builtin_clzll(n.c) >> 3)};
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
        res = _mm_crc32_u64(res, key.items[0]);
        res = _mm_crc32_u64(res, key.items[1]);
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
struct StringHashTableEmpty //-V730
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

    Cell * zeroValue() { return std::launder(reinterpret_cast<Cell *>(&zero_value_storage)); }
    const Cell * zeroValue() const { return std::launder(reinterpret_cast<const Cell *>(&zero_value_storage)); }

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder &&, LookupResult & it, bool & inserted, size_t = 0)
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
    LookupResult ALWAYS_INLINE find(const Key &, size_t = 0)
    {
        return hasZero() ? zeroValue() : nullptr;
    }

    template <typename Key>
    ConstLookupResult ALWAYS_INLINE find(const Key &, size_t = 0) const
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

template <typename Mapped>
struct StringHashTableLookupResult
{
    Mapped * mapped_ptr;
    StringHashTableLookupResult() {}
    StringHashTableLookupResult(Mapped * mapped_ptr_) : mapped_ptr(mapped_ptr_) {}
    StringHashTableLookupResult(std::nullptr_t) {}
    const VoidKey getKey() const { return {}; }
    auto & getMapped() { return *mapped_ptr; }
    auto & operator*() { return *this; }
    auto & operator*() const { return *this; }
    auto * operator->() { return this; }
    auto * operator->() const { return this; }
    operator bool() const { return mapped_ptr; }
    friend bool operator==(const StringHashTableLookupResult & a, const std::nullptr_t &) { return !a.mapped_ptr; }
    friend bool operator==(const std::nullptr_t &, const StringHashTableLookupResult & b) { return !b.mapped_ptr; }
    friend bool operator!=(const StringHashTableLookupResult & a, const std::nullptr_t &) { return a.mapped_ptr; }
    friend bool operator!=(const std::nullptr_t &, const StringHashTableLookupResult & b) { return b.mapped_ptr; }
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
    using mapped_type = typename Ts::mapped_type;
    using value_type = typename Ts::value_type;
    using cell_type = typename Ts::cell_type;

    using LookupResult = StringHashTableLookupResult<typename cell_type::mapped_type>;
    using ConstLookupResult = StringHashTableLookupResult<const typename cell_type::mapped_type>;

    StringHashTable() = default;

    StringHashTable(size_t reserve_for_num_elements)
        : m1{reserve_for_num_elements / 4}
        , m2{reserve_for_num_elements / 4}
        , m3{reserve_for_num_elements / 4}
        , ms{reserve_for_num_elements / 4}
    {
    }

    StringHashTable(StringHashTable && rhs)
        : m1(std::move(rhs.m1))
        , m2(std::move(rhs.m2))
        , m3(std::move(rhs.m3))
        , ms(std::move(rhs.ms))
    {
    }

    ~StringHashTable() = default;

public:
    // Dispatch is written in a way that maximizes the performance:
    // 1. Always memcpy 8 times bytes
    // 2. Use switch case extension to generate fast dispatching table
    // 3. Funcs are named callables that can be force_inlined
    // NOTE: It relies on Little Endianness
    template <typename Self, typename KeyHolder, typename Func>
    static auto ALWAYS_INLINE dispatch(Self & self, KeyHolder && key_holder, Func && func)
    {
        StringHashTableHash hash;
        const StringRef & x = keyHolderGetKey(key_holder);
        const size_t sz = x.size;
        if (sz == 0)
        {
            keyHolderDiscardKey(key_holder);
            return func(self.m0, VoidKey{}, 0);
        }

        if (x.data[sz - 1] == 0)
        {
            // Strings with trailing zeros are not representable as fixed-size
            // string keys. Put them to the generic table.
            return func(self.ms, std::forward<KeyHolder>(key_holder), hash(x));
        }

        const char * p = x.data;
        // pending bits that needs to be shifted out
        const char s = (-sz & 7) * 8;
        union
        {
            StringKey8 k8;
            StringKey16 k16;
            StringKey24 k24;
            UInt64 n[3];
        };
        switch ((sz - 1) >> 3)
        {
            case 0: // 1..8 bytes
            {
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
                return func(self.m1, k8, hash(k8));
            }
            case 1: // 9..16 bytes
            {
                memcpy(&n[0], p, 8);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[1], lp, 8);
                n[1] >>= s;
                keyHolderDiscardKey(key_holder);
                return func(self.m2, k16, hash(k16));
            }
            case 2: // 17..24 bytes
            {
                memcpy(&n[0], p, 16);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[2], lp, 8);
                n[2] >>= s;
                keyHolderDiscardKey(key_holder);
                return func(self.m3, k24, hash(k24));
            }
            default: // >= 25 bytes
            {
                return func(self.ms, std::forward<KeyHolder>(key_holder), hash(x));
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
            mapped = &result->getMapped();
        }
    };

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        this->dispatch(*this, key_holder, EmplaceCallable(it, inserted));
    }

    struct FindCallable
    {
        // find() doesn't need any key memory management, so we don't work with
        // any key holders here, only with normal keys. The key type is still
        // different for every subtable, this is why it is a template parameter.
        template <typename Submap, typename SubmapKey>
        auto ALWAYS_INLINE operator()(Submap & map, const SubmapKey & key, size_t hash)
        {
            auto it = map.find(key, hash);
            if (!it)
                return decltype(&it->getMapped()){};
            else
                return &it->getMapped();
        }
    };

    LookupResult ALWAYS_INLINE find(const Key & x)
    {
        return dispatch(*this, x, FindCallable{});
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & x) const
    {
        return dispatch(*this, x, FindCallable{});
    }

    bool ALWAYS_INLINE has(const Key & x, size_t = 0) const
    {
        return dispatch(*this, x, FindCallable{}) != nullptr;
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
