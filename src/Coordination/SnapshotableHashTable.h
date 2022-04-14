#pragma once
#include <base/StringRef.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/ArenaUtils.h>
#include <unordered_map>
#include <list>
#include <atomic>
#include <iostream>

namespace DB
{

template<typename V>
struct ListNode
{
    StringRef key;
    V value;

    /// Monotonically increasing version info for snapshot
    size_t version{0};
    bool active_in_map{true};
    bool free_key{false};
};

template <class V>
class SnapshotableHashTable
{
private:

    using ListElem = ListNode<V>;
    using List = std::list<ListElem>;
    using Mapped = typename List::iterator;
    using IndexMap = HashMap<StringRef, Mapped>;

    List list;
    IndexMap map;
    bool snapshot_mode{false};
    /// Allows to avoid additional copies in updateValue function
    size_t current_version{0};
    size_t snapshot_up_to_version{0};
    ArenaWithFreeLists arena;
    /// Collect invalid iterators to avoid traversing the whole list
    std::vector<Mapped> snapshot_invalid_iters;

    uint64_t approximate_data_size{0};

    enum OperationType
    {
        INSERT = 0,
        INSERT_OR_REPLACE = 1,
        ERASE = 2,
        UPDATE_VALUE = 3,
        GET_VALUE = 4,
        FIND = 5,
        CONTAINS = 6,
        CLEAR = 7,
        CLEAR_OUTDATED_NODES = 8
    };

    /// Update hash table approximate data size
    ///    op_type: operation type
    ///    key_size: key size
    ///    value_size: size of value to add
    ///    old_value_size: size of value to minus
    /// old_value_size=0 means there is no old value with the same key.
    void updateDataSize(OperationType op_type, uint64_t key_size, uint64_t value_size, uint64_t old_value_size)
    {
        switch (op_type)
        {
            case INSERT:
                approximate_data_size += key_size;
                approximate_data_size += value_size;
                break;
            case INSERT_OR_REPLACE:
                /// replace
                if (old_value_size != 0)
                {
                    approximate_data_size += key_size;
                    approximate_data_size += value_size;
                    if (!snapshot_mode)
                    {
                        approximate_data_size -= key_size;
                        approximate_data_size -= old_value_size;
                    }
                }
                /// insert
                else
                {
                    approximate_data_size += key_size;
                    approximate_data_size += value_size;
                }
                break;
            case UPDATE_VALUE:
                approximate_data_size += key_size;
                approximate_data_size += value_size;
                if (!snapshot_mode)
                {
                    approximate_data_size -= key_size;
                    approximate_data_size -= old_value_size;
                }
                break;
            case ERASE:
                if (!snapshot_mode)
                {
                    approximate_data_size -= key_size;
                    approximate_data_size -= old_value_size;
                }
                break;
            case CLEAR:
                approximate_data_size = 0;
                break;
            case CLEAR_OUTDATED_NODES:
                approximate_data_size -= key_size;
                approximate_data_size -= value_size;
                break;
            default:
                break;
        }
    }

public:

    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using ValueUpdater = std::function<void(V & value)>;

    std::pair<typename IndexMap::LookupResult, bool> insert(const std::string & key, const V & value)
    {
        size_t hash_value = map.hash(key);
        auto it = map.find(key, hash_value);

        if (!it)
        {
            ListElem elem{copyStringInArena(arena, key), value, current_version};
            auto itr = list.insert(list.end(), std::move(elem));
            bool inserted;
            map.emplace(itr->key, it, inserted, hash_value);
            assert(inserted);

            it->getMapped() = itr;
            updateDataSize(INSERT, key.size(), value.sizeInBytes(), 0);
            return std::make_pair(it, true);
        }

        return std::make_pair(it, false);
    }

    void insertOrReplace(const std::string & key, const V & value)
    {
        size_t hash_value = map.hash(key);
        auto it = map.find(key, hash_value);
        uint64_t old_value_size = it == map.end() ? 0 : it->getMapped()->value.sizeInBytes();

        if (it == map.end())
        {
            ListElem elem{copyStringInArena(arena, key), value, current_version};
            auto itr = list.insert(list.end(), std::move(elem));
            bool inserted;
            map.emplace(itr->key, it, inserted, hash_value);
            assert(inserted);
            it->getMapped() = itr;
        }
        else
        {
            auto list_itr = it->getMapped();
            if (snapshot_mode)
            {
                ListElem elem{list_itr->key, value, current_version};
                list_itr->active_in_map = false;
                auto new_list_itr = list.insert(list.end(), std::move(elem));
                it->getMapped() = new_list_itr;
                snapshot_invalid_iters.push_back(list_itr);
            }
            else
            {
                list_itr->value = value;
            }
        }
        updateDataSize(INSERT_OR_REPLACE, key.size(), value.sizeInBytes(), old_value_size);
    }

    bool erase(const std::string & key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return false;

        auto list_itr = it->getMapped();
        uint64_t old_data_size = list_itr->value.sizeInBytes();
        if (snapshot_mode)
        {
            list_itr->active_in_map = false;
            snapshot_invalid_iters.push_back(list_itr);
            list_itr->free_key = true;
            map.erase(it->getKey());
        }
        else
        {
            map.erase(it->getKey());
            arena.free(const_cast<char *>(list_itr->key.data), list_itr->key.size);
            list.erase(list_itr);
        }

        updateDataSize(ERASE, key.size(), 0, old_data_size);
        return true;
    }

    bool contains(const std::string & key) const
    {
        return map.find(key) != map.end();
    }

    const_iterator updateValue(StringRef key, ValueUpdater updater)
    {
        size_t hash_value = map.hash(key);
        auto it = map.find(key, hash_value);
        assert(it != map.end());

        auto list_itr = it->getMapped();
        uint64_t old_value_size = list_itr->value.sizeInBytes();

        const_iterator ret;

        if (snapshot_mode)
        {
            /// We in snapshot mode but updating some node which is already more
            /// fresh than snapshot distance. So it will not participate in
            /// snapshot and we don't need to copy it.
            if (list_itr->version <= snapshot_up_to_version)
            {
                auto elem_copy = *(list_itr);
                list_itr->active_in_map = false;
                snapshot_invalid_iters.push_back(list_itr);
                updater(elem_copy.value);
                elem_copy.version = current_version;
                auto itr = list.insert(list.end(), std::move(elem_copy));
                it->getMapped() = itr;
                ret = itr;
            }
            else
            {
                updater(list_itr->value);
                ret = list_itr;
            }
        }
        else
        {
            updater(list_itr->value);
            ret = list_itr;
        }

        updateDataSize(UPDATE_VALUE, key.size, ret->value.sizeInBytes(), old_value_size);
        return ret;
    }

    const_iterator find(StringRef key) const
    {
        auto map_it = map.find(key);
        if (map_it != map.end())
            return map_it->getMapped();
        return list.end();
    }


    const V & getValue(StringRef key) const
    {
        auto it = map.find(key);
        assert(it);
        return it->getMapped()->value;
    }

    void clearOutdatedNodes()
    {
        for (auto & itr: snapshot_invalid_iters)
        {
            assert(!itr->active_in_map);
            updateDataSize(CLEAR_OUTDATED_NODES, itr->key.size, itr->value.sizeInBytes(), 0);
            if (itr->free_key)
                arena.free(const_cast<char *>(itr->key.data), itr->key.size);
            list.erase(itr);
        }
        snapshot_invalid_iters.clear();
    }

    void clear()
    {
        map.clear();
        for (auto itr = list.begin(); itr != list.end(); ++itr)
            arena.free(const_cast<char *>(itr->key.data), itr->key.size);
        list.clear();
        updateDataSize(CLEAR, 0, 0, 0);
    }

    void enableSnapshotMode(size_t version)
    {
        snapshot_mode = true;
        snapshot_up_to_version = version;
        ++current_version;
    }

    void disableSnapshotMode()
    {
        snapshot_mode = false;
    }

    size_t size() const
    {
        return map.size();
    }

    std::pair<size_t, size_t> snapshotSizeWithVersion() const
    {
        return std::make_pair(list.size(), current_version);
    }

    uint64_t getApproximateDataSize() const
    {
        return approximate_data_size;
    }

    uint64_t keyArenaSize() const
    {
        return arena.size();
    }

    iterator begin() { return list.begin(); }
    const_iterator begin() const { return list.cbegin(); }
    iterator end() { return list.end(); }
    const_iterator end() const { return list.cend(); }
};


}
