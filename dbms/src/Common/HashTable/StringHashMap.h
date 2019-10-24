#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/StringHashTable.h>

template <typename Key, typename TMapped>
struct StringHashMapCell : public HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
};

template<typename Key, typename Mapped>
auto lookupResultGetMapped(StringHashMapCell<Key, Mapped> * cell) { return &cell->getSecond(); }

template <typename TMapped>
struct StringHashMapCell<StringKey16, TMapped> : public HashMapCell<StringKey16, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<StringKey16, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->value.first, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey16 & key, const HashTableNoState & /*state*/) { return key.low == 0; }
    void setZero() { this->value.first.low = 0; }
};

template <typename TMapped>
struct StringHashMapCell<StringKey24, TMapped> : public HashMapCell<StringKey24, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<StringKey24, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->value.first, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey24 & key, const HashTableNoState & /*state*/) { return key.a == 0; }
    void setZero() { this->value.first.a = 0; }
};

template <typename TMapped>
struct StringHashMapCell<StringRef, TMapped> : public HashMapCellWithSavedHash<StringRef, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCellWithSavedHash<StringRef, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
};

template <typename TMapped, typename Allocator>
struct StringHashMapSubMaps
{
    using T0 = StringHashTableEmpty<StringHashMapCell<StringRef, TMapped>>;
    using T1 = HashMapTable<StringKey8, StringHashMapCell<StringKey8, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T2 = HashMapTable<StringKey16, StringHashMapCell<StringKey16, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashMapTable<StringKey24, StringHashMapCell<StringKey24, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using Ts = HashMapTable<StringRef, StringHashMapCell<StringRef, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
};

template <typename TMapped, typename Allocator = HashTableAllocator>
class StringHashMap : public StringHashTable<StringHashMapSubMaps<TMapped, Allocator>>
{
public:
    using Base = StringHashTable<StringHashMapSubMaps<TMapped, Allocator>>;
    using Self = StringHashMap;
    using Key = StringRef;
    using key_type = StringRef;
    using mapped_type = TMapped;
    using value_type = typename Base::Ts::value_type;
    using LookupResult = mapped_type *;

    using Base::Base;

    /// Merge every cell's value of current map into the destination map.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        if (this->m0.hasZero())
        {
            const bool emplace_new_zero = !that.m0.hasZero();
            if (emplace_new_zero)
            {
                that.m0.setHasZero();
            }

            func(that.m0.zeroValue()->getSecond(), this->m0.zeroValue()->getSecond(),
                 emplace_new_zero);
        }

        this->m1.mergeToViaEmplace(that.m1, func);
        this->m2.mergeToViaEmplace(that.m2, func);
        this->m3.mergeToViaEmplace(that.m3, func);
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
        if (this->m0.hasZero())
        {
            if (that.m0.hasZero())
            {
                func(that.m0.zeroValue()->getSecond(), this->m0.zeroValue()->getSecond(), true);
            }
            else
            {
                func(this->m0.zeroValue()->getSecond(), this->m0.zeroValue()->getSecond(), false);
            }
        }

        this->m1.mergeToViaFind(that.m1, func);
        this->m2.mergeToViaFind(that.m2, func);
        this->m3.mergeToViaFind(that.m3, func);
        this->ms.mergeToViaFind(that.ms, func);
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        bool inserted;
        LookupResult it = nullptr;
        emplace(x, it, inserted);
        if (inserted)
            new (it) mapped_type();
        return *it;
    }

    template <typename Func>
    void ALWAYS_INLINE forEachValue(Func && func)
    {
        if (this->m0.size())
        {
            func(StringRef{}, this->m0.zeroValue()->getSecond());
        }

        for (auto & v : this->m1)
        {
            func(toStringRef(v.getFirst()), v.getSecond());
        }

        for (auto & v : this->m2)
        {
            func(toStringRef(v.getFirst()), v.getSecond());
        }

        for (auto & v : this->m3)
        {
            func(toStringRef(v.getFirst()), v.getSecond());
        }

        for (auto & v : this->ms)
        {
            func(v.getFirst(), v.getSecond());
        }
    }

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        if (this->m0.size())
            func(this->m0.zeroValue()->getSecond());
        for (auto & v : this->m1)
            func(v.getSecond());
        for (auto & v : this->m2)
            func(v.getSecond());
        for (auto & v : this->m3)
            func(v.getSecond());
        for (auto & v : this->ms)
            func(v.getSecond());
    }
};
