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

    const VoidKey getKey() const { return VoidKey{}; }
    Mapped & getMapped() { return mapped; }
    const Mapped & getMapped() const { return mapped; }

    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }
};

template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, FixedHashMapCell<Key, Mapped>, Allocator>
{
public:
    using Base = FixedHashTable<Key, FixedHashMapCell<Key, Mapped>, Allocator>;
    using Self = FixedHashMap;
    using key_type = Key;
    using Cell = typename Base::cell_type;
    using value_type = typename Cell::value_type;
    using mapped_type = typename Cell::Mapped;

    using Base::Base;

    using LookupResult = typename Base::LookupResult;

    template <typename TSelf, typename Func>
    static void ALWAYS_INLINE mergeToViaEmplace(TSelf & self, Self & that, Func && func)
    {
        using UCell = std::conditional_t<std::is_const_v<TSelf>, std::add_const_t<Cell>, Cell>;
        self.forEachCell([&](Key i, UCell & cell)
        {
            typename Self::LookupResult res_it;
            bool inserted;
            that.emplace(i, res_it, inserted);
            func(res_it->getMapped(), cell.getMapped(), inserted);
        });
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func) const
    {
        mergeToViaEmplace(*this, that, std::forward<Func>(func));
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        mergeToViaEmplace(*this, that, std::forward<Func>(func));
    }

    template <typename TSelf, typename Func>
    static void ALWAYS_INLINE mergeToViaFind(TSelf & self, Self & that, Func && func)
    {
        using UCell = std::conditional_t<std::is_const_v<TSelf>, std::add_const_t<Cell>, Cell>;
        self.forEachCell([&](Key i, UCell & cell)
        {
            auto & that_cell = that.buf[i];
            if (that_cell.isZero(that))
                func(cell.getMapped(), cell.getMapped(), false);
            else
                func(that_cell.getMapped(), cell.getMapped(), true);
        });
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func) const
    {
        mergeToViaFind(*this, that, std::forward<Func>(func));
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        mergeToViaFind(*this, that, std::forward<Func>(func));
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        typename Base::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (it) mapped_type();

        return it;
    }
};
