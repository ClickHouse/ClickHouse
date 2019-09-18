#pragma once

#include <Common/HashTable/FixedHashTable.h>
#include <Common/HashTable/HashMap.h>


template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    Mapped mapped;
    bool full;

    FixedHashMapCell() {}
    FixedHashMapCell(const Key &, const State &) : full(true) {}
    FixedHashMapCell(const value_type & value_, const State &) : full(true), mapped(value_.second) {}

    Mapped & getSecond() { return mapped; }
    const Mapped & getSecond() const { return mapped; }
    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }
    static constexpr bool need_zero_value_storage = false;
    void setMapped(const value_type & value) { mapped = value.getSecond(); }

    /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped field.
    ///  Note that we have to assemble a continuous layout for the value_type on each call of getValue().
    struct CellExt
    {
        CellExt() {}
        CellExt(Key && key_, const FixedHashMapCell * ptr_) : key(key_), ptr(const_cast<FixedHashMapCell *>(ptr_)) {}
        void update(Key && key_, const FixedHashMapCell * ptr_)
        {
            key = key_;
            ptr = const_cast<FixedHashMapCell *>(ptr_);
        }
        Key key;
        FixedHashMapCell * ptr;

        const Key & getFirst() const { return key; }
        Mapped & getSecond() { return ptr->mapped; }
        const Mapped & getSecond() const { return ptr->mapped; }
        const value_type getValue() const { return {key, ptr->mapped}; }
    };
};


template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, FixedHashMapCell<Key, Mapped>, Allocator>
{
public:
    using Base = FixedHashTable<Key, FixedHashMapCell<Key, Mapped>, Allocator>;
    using Self = FixedHashMap;
    using key_type = Key;
    using mapped_type = Mapped;
    using Cell = typename Base::cell_type;
    using value_type = typename Cell::value_type;

    using Base::Base;

    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            decltype(it) res_it;
            bool inserted;
            that.emplace(it->getFirst(), res_it, inserted, it.getHash());
            func(res_it->getSecond(), it->getSecond(), inserted);
        }
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            decltype(it) res_it = that.find(it->getFirst(), it.getHash());
            if (res_it == that.end())
                func(it->getSecond(), it->getSecond(), false);
            else
                func(res_it->getSecond(), it->getSecond(), true);
        }
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v.getFirst(), v.getSecond());
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
            func(v.getSecond());
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        typename Base::iterator it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getSecond()) mapped_type();

        return it->getSecond();
    }
};
