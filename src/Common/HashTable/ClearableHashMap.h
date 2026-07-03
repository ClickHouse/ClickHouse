#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/ClearableHashSet.h>


template <typename Key, typename Mapped, typename Hash>
struct ClearableHashMapCell : public ClearableHashTableCell<Key, HashMapCell<Key, Mapped, Hash, ClearableHashSetState>>
{
    using Base = ClearableHashTableCell<Key, HashMapCell<Key, Mapped, Hash, ClearableHashSetState>>;
    using Base::Base;

    ClearableHashMapCell(const typename Base::value_type & value_, const typename Base::State & state)
        : Base::BaseCell(value_, state), Base::version(state.version) {}
};

template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
class ClearableHashMap : public HashTable<Key, ClearableHashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>
{
public:
    Mapped & operator[](const Key & x)
    {
        typename ClearableHashMap::LookupResult it;
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

template <typename Key, typename Mapped, typename Hash,
    size_t initial_size_degree>
using ClearableHashMapWithStackMemory = ClearableHashMap<
    Key,
    Mapped,
    Hash,
    HashTableGrower<initial_size_degree>,
    HashTableAllocatorWithStackMemory<
        (1ULL << initial_size_degree)
        * sizeof(ClearableHashMapCell<Key, Mapped, Hash>)>>;
