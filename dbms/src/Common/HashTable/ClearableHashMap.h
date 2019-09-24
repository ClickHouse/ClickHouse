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

template<typename Key, typename Mapped, typename Hash>
ALWAYS_INLINE inline auto lookupResultGetKey(ClearableHashMapCell<Key, Mapped, Hash> * cell) { return &cell->getFirst(); }

template<typename Key, typename Mapped, typename Hash>
ALWAYS_INLINE inline auto lookupResultGetMapped(ClearableHashMapCell<Key, Mapped, Hash> * cell) { return &cell->getSecond(); }

template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class ClearableHashMap : public HashTable<Key, ClearableHashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>
{
public:
    using key_type = Key;
    using mapped_type = Mapped;
    using value_type = typename ClearableHashMap::cell_type::value_type;

    mapped_type & operator[](Key x)
    {
        typename ClearableHashMap::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new(lookupResultGetMapped(it)) mapped_type();

        return *lookupResultGetMapped(it);
    }

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
