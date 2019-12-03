#pragma once

#include <Common/HashTable/FixedHashTable.h>
#include <Common/HashTable/HashMap.h>


template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = TMapped;

    bool full;
    Mapped mapped;

    FixedHashMapCell() {}
    FixedHashMapCell(const Key &, const State &) : full(true) {}
    FixedHashMapCell(const value_type & value_, const State &) : full(true), mapped(value_.second) {}

    const VoidKey getKey() const { return {}; }
    Mapped & getMapped() { return mapped; }
    const Mapped & getMapped() const { return mapped; }

    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }

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

        const Key & getKey() const { return key; }
        Mapped & getMapped() { return ptr->mapped; }
        const Mapped & getMapped() const { return ptr->mapped; }
        const value_type getValue() const { return {key, ptr->mapped}; }
    };
};

template <typename Key, typename Mapped, typename Cell = FixedHashMapCell<Key, Mapped>, typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, Cell, Allocator>
{
public:
    using Base = FixedHashTable<Key, Cell, Allocator>;
    using Self = FixedHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            typename Self::LookupResult res_it;
            bool inserted;
            that.emplace(it->getKey(), res_it, inserted, it.getHash());
            func(res_it->getMapped(), it->getMapped(), inserted);
        }
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            auto res_it = that.find(it->getKey(), it.getHash());
            if (!res_it)
                func(it->getMapped(), it->getMapped(), false);
            else
                func(res_it->getMapped(), it->getMapped(), true);
        }
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v.getKey(), v.getMapped());
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
            func(v.getMapped());
    }

    Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) Mapped();

        return it->getMapped();
    }
};
