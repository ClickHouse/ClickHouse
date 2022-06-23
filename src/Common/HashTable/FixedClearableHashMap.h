#pragma once

#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/FixedHashMap.h>


template <typename Key, typename TMapped>
struct FixedClearableHashMapCell
{
    using Mapped = TMapped;
    using State = ClearableHashSetState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = Mapped;

    UInt32 version;
    Mapped mapped;

    FixedClearableHashMapCell() {}
    FixedClearableHashMapCell(const Key &, const State & state) : version(state.version) {}
    FixedClearableHashMapCell(const value_type & value_, const State & state) : version(state.version), mapped(value_.second) {}

    const VoidKey getKey() const { return {}; }
    Mapped & getMapped() { return mapped; }
    const Mapped & getMapped() const { return mapped; }

    bool isZero(const State & state) const { return version != state.version; }
    void setZero() { version = 0; }

    struct CellExt
    {
        CellExt() {}
        CellExt(Key && key_, FixedClearableHashMapCell * ptr_) : key(key_), ptr(ptr_) {}
        void update(Key && key_, FixedClearableHashMapCell * ptr_)
        {
            key = key_;
            ptr = ptr_;
        }
        Key key;
        FixedClearableHashMapCell * ptr;
        const Key & getKey() const { return key; }
        Mapped & getMapped() { return ptr->mapped; }
        const Mapped & getMapped() const { return *ptr->mapped; }
        const value_type getValue() const { return {key, *ptr->mapped}; }
    };
};


template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
class FixedClearableHashMap : public FixedHashMap<Key, Mapped, FixedClearableHashMapCell<Key, Mapped>, Allocator>
{
public:
    using Base = FixedHashMap<Key, Mapped, FixedClearableHashMapCell<Key, Mapped>, Allocator>;
    using Self = FixedClearableHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    Mapped & operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) Mapped();

        return it->getMapped();
    }

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
