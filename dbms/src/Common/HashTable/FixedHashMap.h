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
    using Self = FixedHashMap;
    using key_type = Key;
    using mapped_type = Mapped;
    using Cell = typename Base::cell_type;
    using value_type = typename Cell::value_type;

    using Base::Base;

    template <typename Func>
    void mergeToEmplace(Self & that, Func && func)
    {
        auto this_buf = this->buf;
        auto that_buf = that.buf;
        for (auto i = 0ul; i < this->BUFFER_SIZE; ++i)
        {
            if (this_buf[i].isZero(*this))
                continue;
            if (that_buf[i].isZero(that))
            {
                new (&that_buf[i]) Cell(i, that);
                ++that.m_size;
                func(that_buf[i].getSecond(), this_buf[i].getSecond(), true);
            }
            else
                func(that_buf[i].getSecond(), this_buf[i].getSecond(), false);
        }
    }

    template <typename Func>
    void mergeToFind(Self & that, Func && func)
    {
        auto this_buf = this->buf;
        auto that_buf = that.buf;
        for (auto i = 0ul; i < this->BUFFER_SIZE; ++i)
        {
            if (this_buf[i].isZero(*this))
                continue;
            if (that_buf[i].isZero(that))
                func(this_buf[i].getSecond(), this_buf[i].getSecond(), false);
            else
                func(that_buf[i].getSecond(), this_buf[i].getSecond(), true);
        }
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        ValueHolder value;
        auto this_buf = this->buf;
        for (auto i = 0u; i < this->BUFFER_SIZE; ++i)
        {
            if (this_buf[i].isZero(*this))
                continue;
            value = {static_cast<Key>(i), &this_buf[i].getSecond()};
            func(value);
        }
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        auto this_buf = this->buf;
        for (auto i = 0u; i < this->BUFFER_SIZE; ++i)
        {
            if (this_buf[i].isZero(*this))
                continue;
            func(this_buf[i].getSecond());
        }
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

    struct ValueHolder
    {
        Key key;
        mapped_type * mapped;
        auto * operator-> () { return this; }
        mapped_type & getSecond() { return *mapped; }
        const mapped_type & getSecond() const { return *mapped; }
        value_type getValue() const { return {key, *mapped}; }
    };
};
