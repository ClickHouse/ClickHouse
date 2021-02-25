#pragma once
#include <boost/intrusive/trivial_value_traits.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/noncopyable.hpp>
#include <common/StringRef.h>
#include <Common/HashTable/HashMap.h>

namespace ZeroTraits
{

bool check(const std::string & x) { return x.empty(); }

void set(std::string & x) { x.clear(); }

}


namespace DB
{

template <typename TKey, typename TMapped, typename Hash, bool save_hash_in_cell>
struct SnapshotableHashMapCell :
    public std::conditional_t<save_hash_in_cell,
        HashMapCellWithSavedHash<TKey, TMapped, Hash, HashTableNoState>,
        HashMapCell<TKey, TMapped, Hash, HashTableNoState>>
{
public:
    using Key = TKey;

    using Base = std::conditional_t<save_hash_in_cell,
        HashMapCellWithSavedHash<TKey, TMapped, Hash, HashTableNoState>,
        HashMapCell<TKey, TMapped, Hash, HashTableNoState>>;

    using Mapped = typename Base::Mapped;
    using State = typename Base::State;

    using mapped_type = Mapped;
    using key_type = Key;

    static bool constexpr need_to_notify_cell_during_move = true;

    static bool constexpr need_zero_value_storage = false;

    using Base::Base;

    static void move(SnapshotableHashMapCell * __restrict old_location, SnapshotableHashMapCell * __restrict new_location)
    {
        /** We update new location prev and next pointers because during hash table resize
         *  they can be updated during move of another cell.
         */

        new_location->prev = old_location->prev;
        new_location->next = old_location->next;

        SnapshotableHashMapCell * prev = new_location->prev;
        SnapshotableHashMapCell * next = new_location->next;

        /// Updated previous next and next previous nodes of list to point to new location

        if (prev)
            prev->next = new_location;

        if (next)
            next->prev = new_location;
    }

private:
    template<typename, typename, typename, bool>
    friend class SnapshotableHashMapCellNodeTraits;

    SnapshotableHashMapCell * next = nullptr;
    SnapshotableHashMapCell * prev = nullptr;
};

template<typename Key, typename Value, typename Hash, bool save_hash_in_cell>
struct SnapshotableHashMapCellNodeTraits
{
    using node = SnapshotableHashMapCell<Key, Value, Hash, save_hash_in_cell>;
    using node_ptr = SnapshotableHashMapCell<Key, Value, Hash, save_hash_in_cell> *;
    using const_node_ptr = const SnapshotableHashMapCell<Key, Value, Hash, save_hash_in_cell> *;

    static node * get_next(const node * ptr) { return ptr->next; }
    static void set_next(node * __restrict ptr, node * __restrict next) { ptr->next = next; }
    static node * get_previous(const node * ptr) { return ptr->prev; }
    static void set_previous(node * __restrict ptr, node * __restrict prev) { ptr->prev = prev; }
};


template <typename TKey, typename TValue, typename Hash, bool save_hash_in_cells>
class SnapshotableHashMapImpl :
    private HashMapTable<
        TKey,
        SnapshotableHashMapCell<TKey, TValue, Hash, save_hash_in_cells>,
        Hash,
        HashTableGrower<>,
        HashTableAllocator>
{
    using Base = HashMapTable<
        TKey,
        SnapshotableHashMapCell<TKey, TValue, Hash, save_hash_in_cells>,
        Hash,
        HashTableGrower<>,
        HashTableAllocator>;
public:
    using Key = TKey;
    using Value = TValue;

    using Cell = SnapshotableHashMapCell<Key, Value, Hash, save_hash_in_cells>;

    using SnapshotableHashMapCellIntrusiveValueTraits =
        boost::intrusive::trivial_value_traits<
            SnapshotableHashMapCellNodeTraits<Key, Value, Hash, save_hash_in_cells>,
            boost::intrusive::link_mode_type::normal_link>;

    using List = boost::intrusive::list<
        Cell,
        boost::intrusive::value_traits<SnapshotableHashMapCellIntrusiveValueTraits>,
        boost::intrusive::constant_time_size<false>>;

    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using reverse_iterator = typename List::reverse_iterator;
    using const_reverse_iterator = typename List::const_reverse_iterator;

    using Base::Base;

    std::pair<Cell *, bool> insert(const Key & key, const Value & value)
    {
        return emplace(key, value);
    }

    std::pair<Cell *, bool> insert(const Key & key, Value && value)
    {
        return emplace(key, std::move(value));
    }

    template<typename ...Args>
    std::pair<Cell *, bool> emplace(const Key & key, Args&&... args)
    {
        size_t hash_value = Base::hash(key);

        Cell * it = Base::find(key, hash_value);

        if (it)
        {
            return std::make_pair(it, false);
        }
        else
        {
            [[maybe_unused]] bool inserted;
            Base::emplaceNonZero(key, it, inserted, hash_value);

            assert(inserted);

            new (&it->getMapped()) Value(std::forward<Args>(args)...);

            list.insert(list.end(), *it);

            return std::make_pair(it, inserted);
        }
    }

    using Base::find;

    Value & get(const Key & key)
    {
        auto it = Base::find(key);
        assert(it);

        Value & value = it->getMapped();

        return value;
    }

    Value & ALWAYS_INLINE operator[](const Key & key)
    {
        auto [it, inserted] = this->emplace(key);
        if (inserted)
            new (&it->getMapped()) Value();

        return it->getMapped();
    }

    const Value & get(const Key & key) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->get(key);
    }

    bool contains(const Key & key) const
    {
        return Base::has(key);
    }

    bool erase(const Key & key)
    {
        auto hash = Base::hash(key);
        auto it = Base::find(key, hash);

        if (!it)
            return false;

        list.erase(list.iterator_to(*it));

        return Base::erase(key, hash);
    }

    void clear()
    {
        list.clear();
        Base::clear();
    }

    using Base::size;

    iterator begin() { return list.begin(); }
    const_iterator begin() const { return list.cbegin(); }
    iterator end() { return list.end(); }
    const_iterator end() const { return list.cend(); }

    reverse_iterator rbegin() { return list.rbegin(); }
    const_reverse_iterator rbegin() const { return list.crbegin(); }
    reverse_iterator rend() { return list.rend(); }
    const_reverse_iterator rend() const { return list.crend(); }

private:
    List list;
};

template <typename Value>
using SnapshotableHashMap = SnapshotableHashMapImpl<StringRef, Value, StringRefHash, false>;

}
