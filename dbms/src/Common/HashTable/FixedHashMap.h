#pragma once

#include <Common/HashTable/FixedHashTable.h>
#include <Common/HashTable/HashMap.h>


template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    bool full;
    Mapped mapped;

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

        Key & getFirstMutable() { return key; }
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
    using key_type = Key;
    using mapped_type = Mapped;
    using value_type = typename Base::cell_type::value_type;

    using Base::Base;

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
