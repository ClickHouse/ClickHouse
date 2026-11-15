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

template <typename Key, typename Mapped, typename Hash>
struct ClearableHashMapCellWithSavedHash
    : public ClearableHashTableCell<Key, HashMapCellWithSavedHash<Key, Mapped, Hash, ClearableHashSetState>>
{
    using Base = ClearableHashTableCell<Key, HashMapCellWithSavedHash<Key, Mapped, Hash, ClearableHashSetState>>;
    using Base::Base;

    ClearableHashMapCellWithSavedHash(const typename Base::value_type & value_, const typename Base::State & state)
        : Base::BaseCell(value_, state)
        , Base::version(state.version)
    {
    }
};

template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Cell = ClearableHashMapCell<Key, Mapped, Hash>,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
class ClearableHashMap : public HashTable<Key, Cell, Hash, Grower, Allocator>
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

    void ALWAYS_INLINE insertIfNotPresent(const Key & x, size_t hash, const typename Cell::Mapped & value)
    {
        typename ClearableHashMap::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted, hash);
        if (inserted)
        {
            new (&it->getMapped()) typename Cell::Mapped();
            it->getMapped() = value;
        }
    }
};

template <typename Key, typename Mapped, typename Hash, size_t initial_size_degree>
using ClearableHashMapWithStackMemory = ClearableHashMap<
    Key,
    Mapped,
    Hash,
    ClearableHashMapCell<Key, Mapped, Hash>,
    HashTableGrower<initial_size_degree>,
    HashTableAllocatorWithStackMemory<(1ULL << initial_size_degree) * sizeof(ClearableHashMapCell<Key, Mapped, Hash>)>>;

template <typename Key, typename Mapped, typename Hash, size_t initial_size_degree>
using ClearableHashMapWithStackMemoryAndSavedHash = ClearableHashMap<
    Key,
    Mapped,
    Hash,
    ClearableHashMapCellWithSavedHash<Key, Mapped, Hash>,
    HashTableGrower<initial_size_degree>,
    HashTableAllocatorWithStackMemory<(1ULL << initial_size_degree) * sizeof(ClearableHashMapCellWithSavedHash<Key, Mapped, Hash>)>>;
