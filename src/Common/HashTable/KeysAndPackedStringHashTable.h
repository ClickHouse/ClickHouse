#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/KeysAndPackedString.h>
#include <Common/HashTable/StringHashTable.h>
#include <Common/HashTable/TwoLevelHashTable.h>

#include <boost/noncopyable.hpp>


/// The hash of a key with a non-empty string: the hash that the key stores (see `KeysAndPackedString`).
struct KeysAndPackedStringHash
{
    template <typename Keys>
    size_t ALWAYS_INLINE operator()(const KeysAndPackedString<Keys> & key) const
    {
        return key.getHash();
    }
};


/// The base of `KeysAndPackedStringCell`: a set cell when `TMapped` is `VoidMapped`, a map cell otherwise.
template <typename Keys, typename TMapped>
using KeysAndPackedStringCellBase = std::conditional_t<
    std::is_same_v<TMapped, VoidMapped>,
    HashTableCell<KeysAndPackedString<Keys>, KeysAndPackedStringHash>,
    HashMapCell<KeysAndPackedString<Keys>, TMapped, KeysAndPackedStringHash>>;

/// A cell of the sub-table of keys with a non-empty string. The empty string marks a free cell: a key with an
/// empty string is stored in the other sub-table, so no key of this one needs the separate storage of the
/// zero value.
template <typename Keys, typename TMapped>
struct KeysAndPackedStringCell : public KeysAndPackedStringCellBase<Keys, TMapped>
{
    using Key = KeysAndPackedString<Keys>;
    using Base = KeysAndPackedStringCellBase<Keys, TMapped>;
    using Base::Base;

    static constexpr bool need_zero_value_storage = false;

    bool isZero(const HashTableNoState & state) const { return isZero(this->getKey(), state); }
    static bool isZero(const Key & key_, const HashTableNoState &) { return key_.str.isEmpty(); }

    void setZero()
    {
        if constexpr (std::is_same_v<TMapped, VoidMapped>)
            this->key.str = {};
        else
            this->value.first.str = {};
    }
};


template <typename Keys, typename TMapped, typename KeysHash, typename Allocator, size_t bits_for_bucket>
class TwoLevelKeysAndPackedStringHashTable;


/** The hash table of a `GROUP BY` over fixed-width keys and one `String`, keyed by
  * `KeysAndPackedString<Keys>`: a map, or a set when `TMapped` is `VoidMapped`, for a `GROUP BY` without
  * aggregate functions. A set's cells hold the keys alone. `KeysAndPackedStringHashMap` and
  * `KeysAndPackedStringHashSet` name the two kinds.
  *
  * Like `StringHashTable`, it splits the keys over sub-tables by their string. A key with an empty string
  * is stored by its fixed-width part alone in `m0`, a table of the `keys32`, `keys64` or `keys128` kind. The
  * other keys are stored whole in `m1`. Empty strings are common in real data, as missing values, and the
  * cells of `m0` are as narrow as fixed-width keys.
  *
  * The hash of a key is the hash of its sub-table: `KeysHash` of the fixed-width part for an empty
  * string, and otherwise the hash that the key stores. The operations that take a saved hash
  * expect this one.
  */
template <
    typename Keys,
    typename TMapped,
    typename KeysHash,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
class KeysAndPackedStringHashTable : private boost::noncopyable
{
public:
    using Key = KeysAndPackedString<Keys>;
    using key_type = Key;
    using mapped_type = TMapped;

    static constexpr bool is_set = std::is_same_v<TMapped, VoidMapped>;

    /// The keys with an empty string, by their fixed-width part.
    using T0 = std::conditional_t<
        is_set,
        HashSetTable<Keys, HashTableCell<Keys, KeysHash>, KeysHash, Grower, Allocator>,
        HashMapTable<Keys, HashMapCell<Keys, TMapped, KeysHash>, KeysHash, Grower, Allocator>>;
    /// The keys with a non-empty string.
    using T1 = std::conditional_t<
        is_set,
        HashSetTable<Key, KeysAndPackedStringCell<Keys, TMapped>, KeysAndPackedStringHash, Grower, Allocator>,
        HashMapTable<Key, KeysAndPackedStringCell<Keys, TMapped>, KeysAndPackedStringHash, Grower, Allocator>>;

    using value_type = typename T1::value_type;
    using cell_type = typename T1::cell_type;

    /// A map hands out the mapped value of a key. A set has none: the address of the key's cell only tells
    /// whether the key is there.
    using LookupResult = std::conditional_t<is_set, const void *, StringHashTableLookupResult<TMapped>>;
    using ConstLookupResult = std::conditional_t<is_set, const void *, StringHashTableLookupResult<const TMapped>>;

protected:
    template <typename, typename, typename, typename, size_t>
    friend class TwoLevelKeysAndPackedStringHashTable;

    T0 m0;
    T1 m1;

    template <size_t key_class>
    auto & submapForClass()
    {
        if constexpr (key_class == 0)
            return m0;
        else
            return m1;
    }

    template <size_t key_class>
    const auto & submapForClass() const
    {
        if constexpr (key_class == 0)
            return m0;
        else
            return m1;
    }

    /// The hash of a key in its sub-table, from the key as the sub-table stores it.
    struct SubmapHash
    {
        size_t ALWAYS_INLINE operator()(const Keys & keys) const { return KeysHash{}(keys); }
        size_t ALWAYS_INLINE operator()(const Key & key) const { return KeysAndPackedStringHash{}(key); }
    };

    /// Calls `func(sub_map, sub_key, hash)` for the sub-table that stores the key. For an empty string it is
    /// `m0` with the fixed-width part of the key, and the holder is discarded; otherwise `m1` with the holder
    /// itself. `resolver(key_class, hash)` returns the sub-table of the class (0 or 1), and
    /// `hash_of(sub_key)` the hash of the key in it.
    template <typename Resolver, typename KeyHolder, typename HashOf, typename Func>
    static auto ALWAYS_INLINE dispatchOnKeyClass(Resolver && resolver, KeyHolder && key_holder, HashOf && hash_of, Func && func)
    {
        const Key & key = keyHolderGetKey(key_holder);
        if (key.str.isEmpty())
        {
            const Keys keys = key.keys;
            keyHolderDiscardKey(key_holder);
            const size_t hash_value = hash_of(keys);
            return func(resolver(std::integral_constant<size_t, 0>{}, hash_value), keys, hash_value);
        }

        const size_t hash_value = hash_of(key);
        return func(resolver(std::integral_constant<size_t, 1>{}, hash_value), std::forward<KeyHolder>(key_holder), hash_value);
    }

    template <typename Table, typename KeyHolder, typename Func>
    static auto ALWAYS_INLINE dispatch(Table & table, KeyHolder && key_holder, Func && func)
    {
        return dispatchOnKeyClass(
            [&](auto key_class, size_t) -> auto & { return table.template submapForClass<decltype(key_class)::value>(); },
            std::forward<KeyHolder>(key_holder),
            SubmapHash{},
            std::forward<Func>(func));
    }

    /// Same as `dispatch`, but uses a hash saved from an earlier `hash` instead of computing it.
    template <typename Table, typename KeyHolder, typename Func>
    static auto ALWAYS_INLINE dispatchWithHash(Table & table, KeyHolder && key_holder, size_t hash_value, Func && func)
    {
        return dispatchOnKeyClass(
            [&](auto key_class, size_t) -> auto & { return table.template submapForClass<decltype(key_class)::value>(); },
            std::forward<KeyHolder>(key_holder),
            [hash_value](const auto &) { return hash_value; },
            std::forward<Func>(func));
    }

    struct EmplaceCallable
    {
        LookupResult & it;
        bool & inserted;

        template <typename Map, typename KeyHolder>
        void ALWAYS_INLINE operator()(Map & map, KeyHolder && key_holder, size_t hash_value)
        {
            typename Map::LookupResult result;
            map.emplace(key_holder, result, inserted, hash_value);
            if constexpr (is_set)
                it = result;
            else
                it = &result->getMapped();
        }
    };

    struct FindCallable
    {
        template <typename Map, typename SubKey>
        auto ALWAYS_INLINE operator()(Map & map, const SubKey & key, size_t hash_value)
        {
            auto it = map.find(key, hash_value);
            if constexpr (is_set)
                return static_cast<const void *>(it);
            else
            {
                if (!it)
                    return decltype(&it->getMapped()){};
                return &it->getMapped();
            }
        }
    };

    struct PrefetchCallable
    {
        template <typename Map, typename KeyHolder>
        void ALWAYS_INLINE operator()(Map & map, KeyHolder && key_holder, size_t hash_value)
        {
            map.prefetchByHash(hash_value);
            keyHolderDiscardKey(key_holder);
        }
    };

    struct EraseCallable
    {
        template <typename Map, typename SubKey>
        bool ALWAYS_INLINE operator()(Map & map, const SubKey & key, size_t hash_value)
        {
            return map.erase(key, hash_value);
        }
    };

public:
    KeysAndPackedStringHashTable() = default;

    /// Preallocates the sub-tables for `size_hint` keys, `size_hint_with_empty_string` of them with an empty
    /// string.
    KeysAndPackedStringHashTable(size_t size_hint, size_t size_hint_with_empty_string)
        : m0(size_hint_with_empty_string)
        , m1(size_hint - size_hint_with_empty_string)
    {
    }

    KeysAndPackedStringHashTable(KeysAndPackedStringHashTable && rhs) noexcept
        : m0(std::move(rhs.m0))
        , m1(std::move(rhs.m1))
    {
    }

    KeysAndPackedStringHashTable & operator=(KeysAndPackedStringHashTable && rhs) noexcept
    {
        if (this == &rhs)
            return *this;

        m0 = std::move(rhs.m0);
        m1 = std::move(rhs.m1);
        return *this;
    }

    /// The hash that `emplace` and `find` compute for the key, to be saved and passed back to them. It depends on the
    /// key alone, so the staged records of the adaptive aggregator restore it without a table (see
    /// `StagedKeysAndPackedStringKey`).
    static size_t ALWAYS_INLINE hash(const Key & key)
    {
        return key.str.isEmpty() ? KeysHash{}(key.keys) : KeysAndPackedStringHash{}(key);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        dispatch(*this, key_holder, EmplaceCallable{it, inserted});
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash_value)
    {
        dispatchWithHash(*this, key_holder, hash_value, EmplaceCallable{it, inserted});
    }

    LookupResult ALWAYS_INLINE find(const Key & key) { return dispatch(*this, key, FindCallable{}); }
    ConstLookupResult ALWAYS_INLINE find(const Key & key) const { return dispatch(*this, key, FindCallable{}); }

    LookupResult ALWAYS_INLINE find(const Key & key, size_t hash_value)
    {
        return dispatchWithHash(*this, key, hash_value, FindCallable{});
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & key, size_t hash_value) const
    {
        return dispatchWithHash(*this, key, hash_value, FindCallable{});
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder && key_holder) const
    {
        dispatch(*this, std::forward<KeyHolder>(key_holder), PrefetchCallable{});
    }

    /// Prefetches the cell of a key before the key is built: by its saved hash and by whether its string is
    /// empty.
    void ALWAYS_INLINE prefetchByHash(size_t hash_value, bool empty_string) const
    {
        if (empty_string)
            m0.prefetchByHash(hash_value);
        else
            m1.prefetchByHash(hash_value);
    }

    bool ALWAYS_INLINE erase(const Key & key) { return dispatch(*this, key, EraseCallable{}); }

    /// Calls `func(const Key &, TMapped &)` for every element of a map, and `func(const Key &)` for every
    /// element of a set.
    template <typename Func>
    void ALWAYS_INLINE forEachValue(Func && func)
    {
        for (auto & cell : m0)
        {
            if constexpr (is_set)
                func(Key::withEmptyString(cell.getKey()));
            else
                func(Key::withEmptyString(cell.getKey()), cell.getMapped());
        }
        m1.forEachValue(func);
    }

    /// Calls `func(const Key &, TMapped &, size_t hash)` for every element of a map, and `func(const Key &,
    /// size_t hash)` for every element of a set, with the hash of the key in its sub-table (see `hash`). The
    /// cells do not save it, so it is computed again from the key.
    template <typename Func>
    void ALWAYS_INLINE forEachValueWithHash(Func && func)
    {
        for (auto it = m0.begin(), end = m0.end(); it != end; ++it)
        {
            if constexpr (is_set)
                func(Key::withEmptyString(it->getKey()), it.getHash());
            else
                func(Key::withEmptyString(it->getKey()), it->getMapped(), it.getHash());
        }
        for (auto it = m1.begin(), end = m1.end(); it != end; ++it)
        {
            if constexpr (is_set)
                func(it->getKey(), it.getHash());
            else
                func(it->getKey(), it->getMapped(), it.getHash());
        }
    }

    /// Calls `func(TMapped &)` for every element; a `func` returning `bool` stops the iteration by returning
    /// `false`. A set has no mapped values, so it calls nothing, as `HashSetTable::forEachMapped`.
    template <typename Func>
    void ALWAYS_INLINE forEachMapped([[maybe_unused]] Func && func)
    {
        if constexpr (is_set)
            return;
        else if constexpr (std::is_same_v<decltype(func(std::declval<TMapped &>())), bool>)
        {
            for (auto & cell : m0)
                if (!func(cell.getMapped()))
                    return;
            for (auto & cell : m1)
                if (!func(cell.getMapped()))
                    return;
        }
        else
        {
            m0.forEachMapped(func);
            m1.forEachMapped(func);
        }
    }

    /// See `HashMapTable::mergeToViaEmplace`.
    template <typename Func, bool prefetch = false>
    requires(!is_set)
    void ALWAYS_INLINE mergeToViaEmplace(KeysAndPackedStringHashTable & that, Func && func)
    {
        m0.template mergeToViaEmplace<std::remove_reference_t<Func> &, prefetch>(that.m0, func);
        m1.template mergeToViaEmplace<std::remove_reference_t<Func> &, prefetch>(that.m1, func);
    }

    /// See `HashSetTable::mergeToViaEmplace`.
    template <bool prefetch = false>
    requires is_set
    void ALWAYS_INLINE mergeToViaEmplace(KeysAndPackedStringHashTable & that)
    {
        m0.template mergeToViaEmplace<prefetch>(that.m0);
        m1.template mergeToViaEmplace<prefetch>(that.m1);
    }

    /// See `HashMapTable::mergeToViaFind`.
    template <typename Func>
    requires(!is_set)
    void ALWAYS_INLINE mergeToViaFind(KeysAndPackedStringHashTable & that, Func && func)
    {
        m0.mergeToViaFind(that.m0, func);
        m1.mergeToViaFind(that.m1, func);
    }

    /// Visits the sub-tables of two tables of the same type as pairs, in matching order.
    template <typename Func>
    static void forEachSubMapPair(KeysAndPackedStringHashTable & lhs, KeysAndPackedStringHashTable & rhs, Func && func)
    {
        func(lhs.m0, rhs.m0);
        func(lhs.m1, rhs.m1);
    }

    size_t size() const { return m0.size() + m1.size(); }
    /// The number of keys with an empty string.
    size_t sizeWithEmptyString() const { return m0.size(); }
    bool empty() const { return m0.empty() && m1.empty(); }
    size_t getBufferSizeInBytes() const { return m0.getBufferSizeInBytes() + m1.getBufferSizeInBytes(); }

    /// Grows the sub-tables to hold `size_hint` keys, `size_hint_with_empty_string` of them with an empty
    /// string.
    void reserve(size_t size_hint, size_t size_hint_with_empty_string)
    {
        m0.reserve(size_hint_with_empty_string);
        m1.reserve(size_hint - size_hint_with_empty_string);
    }

    /// Removes every element and keeps the buffers.
    void clear()
    {
        m0.clear();
        m1.clear();
    }

    void clearAndShrink()
    {
        m0.clearAndShrink();
        m1.clearAndShrink();
    }
};


/// The two-level form of `KeysAndPackedStringHashTable`: every key is stored in the bucket of its hash.
template <
    typename Keys,
    typename TMapped,
    typename KeysHash,
    typename Allocator = HashTableAllocator,
    size_t bits_for_bucket = DEFAULT_BITS_FOR_BUCKET>
class TwoLevelKeysAndPackedStringHashTable : private boost::noncopyable
{
public:
    using Impl = KeysAndPackedStringHashTable<Keys, TMapped, KeysHash, TwoLevelHashTableGrower<>, Allocator>;

    using Key = typename Impl::Key;
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;
    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << bits_for_bucket;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    Impl impls[NUM_BUCKETS];

    TwoLevelKeysAndPackedStringHashTable() = default;

    /// Preallocates the buckets for `size_hint` keys, `size_hint_with_empty_string` of them with an empty
    /// string, spread evenly over the buckets.
    TwoLevelKeysAndPackedStringHashTable(size_t size_hint, size_t size_hint_with_empty_string)
    {
        for (auto & impl : impls)
            impl.reserve(size_hint / NUM_BUCKETS, size_hint_with_empty_string / NUM_BUCKETS);
    }

    /// Copies the keys of a single-level table into the buckets of their hashes. The constraint keeps an
    /// integer argument from choosing this constructor.
    template <typename Source>
    requires(!std::is_arithmetic_v<Source>)
    explicit TwoLevelKeysAndPackedStringHashTable(const Source & src)
    {
        auto it0 = src.m0.begin();
        /// The zero key comes first in the iteration order. It lives in the separate zero-value storage,
        /// which only `insert` fills.
        if (src.m0.hasZero())
        {
            impls[getBucketFromHash(it0.getHash())].m0.insert(it0->getValue());
            ++it0;
        }

        for (; it0 != src.m0.end(); ++it0)
        {
            const size_t hash_value = it0.getHash();
            impls[getBucketFromHash(hash_value)].m0.insertUniqueNonZero(it0.getPtr(), hash_value);
        }

        for (auto it1 = src.m1.begin(); it1 != src.m1.end(); ++it1)
        {
            const size_t hash_value = it1.getHash();
            impls[getBucketFromHash(hash_value)].m1.insertUniqueNonZero(it1.getPtr(), hash_value);
        }
    }

    /// See `KeysAndPackedStringHashTable::hash`: the hash of a key depends on the key alone.
    static size_t ALWAYS_INLINE hash(const Key & key) { return Impl::hash(key); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - bits_for_bucket)) & MAX_BUCKET; }

    /// The sub-table dispatch of `KeysAndPackedStringHashTable`, resolving the sub-table in the bucket of the
    /// hash.
    template <typename Table, typename KeyHolder, typename Func>
    static auto ALWAYS_INLINE dispatch(Table & table, KeyHolder && key_holder, Func && func)
    {
        return Impl::dispatchOnKeyClass(
            [&](auto key_class, size_t hash_value) -> auto &
            { return table.impls[getBucketFromHash(hash_value)].template submapForClass<decltype(key_class)::value>(); },
            std::forward<KeyHolder>(key_holder),
            typename Impl::SubmapHash{},
            std::forward<Func>(func));
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        dispatch(*this, key_holder, typename Impl::EmplaceCallable{it, inserted});
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash_value)
    {
        impls[getBucketFromHash(hash_value)].emplace(std::forward<KeyHolder>(key_holder), it, inserted, hash_value);
    }

    LookupResult ALWAYS_INLINE find(const Key & key) { return dispatch(*this, key, typename Impl::FindCallable{}); }
    ConstLookupResult ALWAYS_INLINE find(const Key & key) const { return dispatch(*this, key, typename Impl::FindCallable{}); }

    LookupResult ALWAYS_INLINE find(const Key & key, size_t hash_value)
    {
        return impls[getBucketFromHash(hash_value)].find(key, hash_value);
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & key, size_t hash_value) const
    {
        return impls[getBucketFromHash(hash_value)].find(key, hash_value);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder && key_holder) const
    {
        dispatch(*this, std::forward<KeyHolder>(key_holder), typename Impl::PrefetchCallable{});
    }

    bool ALWAYS_INLINE erase(const Key & key) { return dispatch(*this, key, typename Impl::EraseCallable{}); }

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto & impl : impls)
            impl.forEachMapped(func);
    }

    size_t size() const
    {
        size_t res = 0;
        for (const auto & impl : impls)
            res += impl.size();
        return res;
    }

    size_t sizeWithEmptyString() const
    {
        size_t res = 0;
        for (const auto & impl : impls)
            res += impl.sizeWithEmptyString();
        return res;
    }

    bool empty() const
    {
        for (const auto & impl : impls)
            if (!impl.empty())
                return false;
        return true;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t res = 0;
        for (const auto & impl : impls)
            res += impl.getBufferSizeInBytes();
        return res;
    }
};


template <
    typename Keys,
    typename TMapped,
    typename KeysHash,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
using KeysAndPackedStringHashMap = KeysAndPackedStringHashTable<Keys, TMapped, KeysHash, Grower, Allocator>;

template <typename Keys, typename KeysHash, typename Grower = HashTableGrowerWithPrecalculation<>, typename Allocator = HashTableAllocator>
using KeysAndPackedStringHashSet = KeysAndPackedStringHashTable<Keys, VoidMapped, KeysHash, Grower, Allocator>;

template <
    typename Keys,
    typename TMapped,
    typename KeysHash,
    typename Allocator = HashTableAllocator,
    size_t bits_for_bucket = DEFAULT_BITS_FOR_BUCKET>
using TwoLevelKeysAndPackedStringHashMap = TwoLevelKeysAndPackedStringHashTable<Keys, TMapped, KeysHash, Allocator, bits_for_bucket>;

template <typename Keys, typename KeysHash, typename Allocator = HashTableAllocator, size_t bits_for_bucket = DEFAULT_BITS_FOR_BUCKET>
using TwoLevelKeysAndPackedStringHashSet = TwoLevelKeysAndPackedStringHashTable<Keys, VoidMapped, KeysHash, Allocator, bits_for_bucket>;
