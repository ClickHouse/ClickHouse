#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/SerializedHashTable.h>

template <typename Key, typename TMapped>
struct SerializedHashMapCell : public HashMapCell<Key, TMapped, SerializedHashTableHash, HashTableNoState>
{
    static_assert(false, "SerializedHashMapCell must be specialized for this Key type");
};

template <typename TMapped>
struct SerializedHashMapCell<StringRefWithInlineHash, TMapped>
    : public HashMapCell<StringRefWithInlineHash, TMapped, SerializedHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<StringRefWithInlineHash, TMapped, SerializedHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    StringRef getKey() const { return StringRef(this->value.first.data, this->value.first.size >> StringRefWithInlineHash::SHIFT); }

    /// Get the key (internally).
    static const StringRefWithInlineHash & getKey(const value_type & value_) { return value_.first; }

    void setHash(size_t hash_value) { chassert(hash_value == this->value.first.size); }

    bool keyEquals(const StringRefWithInlineHash & key_, size_t /*hash_*/, const Base::State & /* state */) const
    {
        if (Base::value.first.size != key_.size)
            return false;

        size_t size = Base::value.first.size >> StringRefWithInlineHash::SHIFT;
        if (size == 0)
            return true;

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(Base::value.first.data, key_.data, size);
#else
        return 0 == memcmp(Base::value.first.data, key_.data, size);
#endif
    }
    size_t getHash(const SerializedHashTableHash & /*hash_function*/) const { return this->value.first.size; }
};

template <typename TMapped>
struct SerializedHashMapCell<StringRef, TMapped>
    : public HashMapCellWithSavedHash<StringRef, TMapped, SerializedHashTableHash, HashTableNoState>
{
    using Base = HashMapCellWithSavedHash<StringRef, TMapped, SerializedHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    StringRef getKey() const { return this->value.first; } /// NOLINT
    // internal
    static const StringRef & getKey(const value_type & value_) { return value_.first; }
};

template <typename TMapped, typename Allocator>
struct SerializedHashMapSubMaps
{
    using Th = HashMapTable<
        StringRefWithInlineHash,
        SerializedHashMapCell<StringRefWithInlineHash, TMapped>,
        SerializedHashTableHash,
        HashTableGrowerWithPrecalculation<>,
        Allocator>;
    using Ts = HashMapTable<
        StringRef,
        SerializedHashMapCell<StringRef, TMapped>,
        SerializedHashTableHash,
        HashTableGrowerWithPrecalculation<>,
        Allocator>;
};

template <typename TMapped, typename Allocator = HashTableAllocator>
class SerializedHashMap : public SerializedHashTable<SerializedHashMapSubMaps<TMapped, Allocator>>
{
public:
    using Key = StringRef;
    using Base = SerializedHashTable<SerializedHashMapSubMaps<TMapped, Allocator>>;
    using Self = SerializedHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    /// Merge every cell's value of current map into the destination map.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func, bool>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        this->mh.mergeToViaEmplace(that.mh, func);
        this->ms.mergeToViaEmplace(that.ms, func);
    }

    /// Merge every cell's value of current map into the destination map via find.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool exist).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, func is invoked with the third argument
    ///  exist set to false. Otherwise exist is set to true.
    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        this->mh.mergeToViaFind(that.mh, func);
        this->ms.mergeToViaFind(that.ms, func);
    }

    TMapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) TMapped();

        return it->getMapped();
    }

    template <typename Func>
    void ALWAYS_INLINE forEachValue(Func && func)
    {
        for (auto & v : this->mh)
            func(v.getKey(), v.getMapped());

        for (auto & v : this->ms)
            func(v.getKey(), v.getMapped());
    }

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto & v : this->mh)
            func(v.getMapped());
        for (auto & v : this->ms)
            func(v.getMapped());
    }
};
