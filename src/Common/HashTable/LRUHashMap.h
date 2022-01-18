#pragma once

#include <common/types.h>

#include <boost/intrusive/trivial_value_traits.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/noncopyable.hpp>

#include <Core/Defines.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>


template <typename TKey, typename TMapped, typename Hash, bool save_hash_in_cell>
struct LRUHashMapCell :
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

    using Base::Base;

    static bool constexpr need_to_notify_cell_during_move = true;

    static void move(LRUHashMapCell * __restrict old_location, LRUHashMapCell * __restrict new_location)
    {
        /** We update new location prev and next pointers because during hash table resize
         *  they can be updated during move of another cell.
         */

        new_location->prev = old_location->prev;
        new_location->next = old_location->next;

        LRUHashMapCell * prev = new_location->prev;
        LRUHashMapCell * next = new_location->next;

        /// Updated previous next and next previous nodes of list to point to new location

        if (prev)
            prev->next = new_location;

        if (next)
            next->prev = new_location;
    }

private:
    template<typename, typename, typename, bool>
    friend class LRUHashMapCellNodeTraits;

    LRUHashMapCell * next = nullptr;
    LRUHashMapCell * prev = nullptr;
};

template<typename Key, typename Value, typename Hash, bool save_hash_in_cell>
struct LRUHashMapCellNodeTraits
{
    using node = LRUHashMapCell<Key, Value, Hash, save_hash_in_cell>;
    using node_ptr = LRUHashMapCell<Key, Value, Hash, save_hash_in_cell> *;
    using const_node_ptr = const LRUHashMapCell<Key, Value, Hash, save_hash_in_cell> *;

    static node * get_next(const node * ptr) { return ptr->next; }
    static void set_next(node * __restrict ptr, node * __restrict next) { ptr->next = next; }
    static node * get_previous(const node * ptr) { return ptr->prev; }
    static void set_previous(node * __restrict ptr, node * __restrict prev) { ptr->prev = prev; }
};

template <typename TKey, typename TValue, typename Hash, bool save_hash_in_cells>
class LRUHashMapImpl :
    private HashMapTable<
        TKey,
        LRUHashMapCell<TKey, TValue, Hash, save_hash_in_cells>,
        Hash,
        HashTableGrower<>,
        HashTableAllocator>
{
    using Base = HashMapTable<
        TKey,
        LRUHashMapCell<TKey, TValue, Hash, save_hash_in_cells>,
        Hash,
        HashTableGrower<>,
        HashTableAllocator>;
public:
    using Key = TKey;
    using Value = TValue;

    using Cell = LRUHashMapCell<Key, Value, Hash, save_hash_in_cells>;

    using LRUHashMapCellIntrusiveValueTraits =
        boost::intrusive::trivial_value_traits<
            LRUHashMapCellNodeTraits<Key, Value, Hash, save_hash_in_cells>,
            boost::intrusive::link_mode_type::normal_link>;

    using LRUList = boost::intrusive::list<
        Cell,
        boost::intrusive::value_traits<LRUHashMapCellIntrusiveValueTraits>,
        boost::intrusive::constant_time_size<false>>;

    using iterator = typename LRUList::iterator;
    using const_iterator = typename LRUList::const_iterator;
    using reverse_iterator = typename LRUList::reverse_iterator;
    using const_reverse_iterator = typename LRUList::const_reverse_iterator;

    LRUHashMapImpl(size_t max_size_, bool preallocate_max_size_in_hash_map = false)
        : Base(preallocate_max_size_in_hash_map ? max_size_ : 32)
        , max_size(max_size_)
    {
        assert(max_size > 0);
    }

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
            /// Cell contains element return it and put to the end of lru list
            lru_list.splice(lru_list.end(), lru_list, lru_list.iterator_to(*it));
            return std::make_pair(it, false);
        }

        if (size() == max_size)
        {
            /// Erase least recently used element from front of the list
            Cell & node = lru_list.front();

            const Key & element_to_remove_key = node.getKey();
            size_t key_hash = node.getHash(*this);

            lru_list.pop_front();

            [[maybe_unused]] bool erased = Base::erase(element_to_remove_key, key_hash);
            assert(erased);
        }

        [[maybe_unused]] bool inserted;

        /// Insert value first try to insert in zero storage if not then insert in buffer
        if (!Base::emplaceIfZero(key, it, inserted, hash_value))
            Base::emplaceNonZero(key, it, inserted, hash_value);

        assert(inserted);

        new (&it->getMapped()) Value(std::forward<Args>(args)...);

        /// Put cell to the end of lru list
        lru_list.insert(lru_list.end(), *it);

        return std::make_pair(it, true);
    }

    using Base::find;

    Value & get(const Key & key)
    {
        auto it = Base::find(key);
        assert(it);

        Value & value = it->getMapped();

        /// Put cell to the end of lru list
        lru_list.splice(lru_list.end(), lru_list, lru_list.iterator_to(*it));

        return value;
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

        lru_list.erase(lru_list.iterator_to(*it));

        return Base::erase(key, hash);
    }

    void clear()
    {
        lru_list.clear();
        Base::clear();
    }

    using Base::size;

    size_t getMaxSize() const { return max_size; }

    iterator begin() { return lru_list.begin(); }
    const_iterator begin() const { return lru_list.cbegin(); }
    iterator end() { return lru_list.end(); }
    const_iterator end() const { return lru_list.cend(); }

    reverse_iterator rbegin() { return lru_list.rbegin(); }
    const_reverse_iterator rbegin() const { return lru_list.crbegin(); }
    reverse_iterator rend() { return lru_list.rend(); }
    const_reverse_iterator rend() const { return lru_list.crend(); }

private:
    size_t max_size;
    LRUList lru_list;
};

template <typename Key, typename Value, typename Hash = DefaultHash<Key>>
using LRUHashMap = LRUHashMapImpl<Key, Value, Hash, false>;

template <typename Key, typename Value, typename Hash = DefaultHash<Key>>
using LRUHashMapWithSavedHash = LRUHashMapImpl<Key, Value, Hash, true>;
