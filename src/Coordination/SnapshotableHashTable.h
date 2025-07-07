#pragma once
#include <base/StringRef.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ArenaUtils.h>

#include <list>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template<typename V>
struct ListNode
{
    StringRef key;
    V value;

    struct
    {
        uint64_t active_in_map : 1;
        uint64_t free_key : 1;
        uint64_t version : 62;
    } node_metadata{false, false, 0};

    void setInactiveInMap()
    {
        node_metadata.active_in_map = false;
    }

    void setActiveInMap()
    {
        node_metadata.active_in_map = true;
    }

    bool isActiveInMap()
    {
        return node_metadata.active_in_map;
    }

    void setFreeKey()
    {
        node_metadata.free_key = true;
    }

    bool getFreeKey()
    {
        return node_metadata.free_key;
    }

    uint64_t getVersion()
    {
        return node_metadata.version;
    }

    void setVersion(uint64_t version)
    {
        node_metadata.version = version;
    }
};

template <class V>
class SnapshotableHashTable
{
private:
    struct GlobalArena
    {
        char * alloc(const size_t size)
        {
            return new char[size];
        }

        void free(const char * ptr, size_t /*size*/)
        {
            delete [] ptr;
        }
    };

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

    /// Arena used for keys
    /// we don't use std::string because it uses 24 bytes (because of SSO)
    /// we want to always allocate the key on heap and use StringRef to it
    GlobalArena arena;

    /// Collect invalid iterators to avoid traversing the whole list
    std::vector<Mapped> snapshot_invalid_iters;

    uint64_t approximate_data_size{0};

    enum OperationType
    {
        INSERT_OR_REPLACE = 0,
        ERASE = 1,
        UPDATE = 2,
        CLEAR = 3,
    };

    /// Update hash table approximate data size
    ///    op_type: operation type
    ///    key_size: key size
    ///    value_size: size of value to add
    ///    old_value_size: size of value to minus
    /// old_value_size=0 means there is no old value with the same key.
    void updateDataSize(OperationType op_type, uint64_t key_size, uint64_t value_size, uint64_t old_value_size, bool remove_old = true)
    {
        switch (op_type)
        {
            case INSERT_OR_REPLACE:
                approximate_data_size += key_size;
                approximate_data_size += value_size;
                if (remove_old && old_value_size != 0)
                {
                    approximate_data_size -= key_size;
                    approximate_data_size -= old_value_size;
                }
                break;
            case UPDATE:
                approximate_data_size += key_size;
                approximate_data_size += value_size;
                if (remove_old)
                {
                    approximate_data_size -= key_size;
                    approximate_data_size -= old_value_size;
                }
                break;
            case ERASE:
                if (remove_old)
                {
                    approximate_data_size -= key_size;
                    approximate_data_size -= old_value_size;
                }
                break;
            case CLEAR:
                approximate_data_size = 0;
                break;
        }
    }

    void insertOrReplace(StringRef key, V value, bool owns_key)
    {
        size_t hash_value = map.hash(key);
        auto new_value_size = value.sizeInBytes();
        auto it = map.find(key, hash_value);
        uint64_t old_value_size = it == map.end() ? 0 : it->getMapped()->value.sizeInBytes();

        if (it == map.end())
        {
            auto list_key = owns_key ? key : copyStringInArena(arena, key);
            ListElem elem{list_key, std::move(value)};
            elem.setVersion(current_version);
            auto itr = list.insert(list.end(), std::move(elem));
            bool inserted;
            map.emplace(itr->key, it, inserted, hash_value);
            itr->setActiveInMap();
            chassert(inserted);
            it->getMapped() = itr;
        }
        else
        {
            if (owns_key)
                arena.free(key.data, key.size);

            auto list_itr = it->getMapped();
            if (snapshot_mode)
            {
                ListElem elem{list_itr->key, std::move(value)};
                elem.setVersion(current_version);
                list_itr->setInactiveInMap();
                auto new_list_itr = list.insert(list.end(), std::move(elem));
                it->getMapped() = new_list_itr;
                snapshot_invalid_iters.push_back(list_itr);
            }
            else
            {
                list_itr->value = std::move(value);
            }
        }
        updateDataSize(INSERT_OR_REPLACE, key.size, new_value_size, old_value_size, !snapshot_mode);
    }

public:

    using Node = V;
    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using ValueUpdater = std::function<void(V & value)>;

    ~SnapshotableHashTable()
    {
        clear();
    }

    std::pair<typename IndexMap::LookupResult, bool> insert(const std::string & key, const V & value)
    {
        size_t hash_value = map.hash(key);
        auto it = map.find(key, hash_value);

        if (!it)
        {
            ListElem elem{copyStringInArena(arena, key), value};
            elem.setVersion(current_version);
            auto itr = list.insert(list.end(), std::move(elem));
            bool inserted;
            map.emplace(itr->key, it, inserted, hash_value);
            itr->setActiveInMap();
            chassert(inserted);

            it->getMapped() = itr;
            updateDataSize(INSERT_OR_REPLACE, key.size(), value.sizeInBytes(), 0);
            return std::make_pair(it, true);
        }

        return std::make_pair(it, false);
    }

    void reserve(size_t node_num)
    {
        map.reserve(node_num);
    }

    void insertOrReplace(const std::string & key, V value)
    {
        insertOrReplace(key, std::move(value), /*owns_key*/ false);
    }

    struct KeyDeleter
    {
        void operator()(const char * key)
        {
            if (key)
                arena->free(key, size);
        }

        size_t size;
        GlobalArena * arena;
    };

    using KeyPtr = std::unique_ptr<char[], KeyDeleter>;

    KeyPtr allocateKey(size_t size)
    {
        return KeyPtr{new char[size], KeyDeleter{size, &arena}};
    }

    void insertOrReplace(KeyPtr key_data, size_t key_size, V value)
    {
        StringRef key{key_data.release(), key_size};
        insertOrReplace(key, std::move(value), /*owns_key*/ true);
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
            list_itr->setInactiveInMap();
            snapshot_invalid_iters.push_back(list_itr);
            list_itr->setFreeKey();
            map.erase(it->getKey());
        }
        else
        {
            map.erase(it->getKey());
            arena.free(const_cast<char *>(list_itr->key.data), list_itr->key.size);
            list.erase(list_itr);
        }

        updateDataSize(ERASE, key.size(), 0, old_data_size, !snapshot_mode);
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
        if (it == map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find key: '{}'", key.toView());

        auto list_itr = it->getMapped();
        uint64_t old_value_size = list_itr->value.sizeInBytes();

        const_iterator ret;

        bool remove_old_size = true;
        if (snapshot_mode)
        {
            /// We in snapshot mode but updating some node which is already more
            /// fresh than snapshot distance. So it will not participate in
            /// snapshot and we don't need to copy it.
            if (list_itr->getVersion() <= snapshot_up_to_version)
            {
                auto elem_copy = *(list_itr);
                list_itr->setInactiveInMap();
                snapshot_invalid_iters.push_back(list_itr);
                updater(elem_copy.value);

                elem_copy.setVersion(current_version);
                auto itr = list.insert(list.end(), std::move(elem_copy));
                it->getMapped() = itr;
                ret = itr;

                remove_old_size = false;
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

        updateDataSize(UPDATE, key.size, ret->value.sizeInBytes(), old_value_size, remove_old_size);
        return ret;
    }

    const_iterator find(StringRef key) const
    {
        auto map_it = map.find(key);
        if (map_it != map.end())
            /// return std::make_shared<KVPair>(KVPair{map_it->getMapped()->key, map_it->getMapped()->value});
            return map_it->getMapped();
        return list.end();
    }


    const V & getValue(StringRef key) const
    {
        auto it = map.find(key);
        if (it == map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find key: '{}'", key.toView());
        return it->getMapped()->value;
    }

    void clearOutdatedNodes()
    {
        for (auto & itr : snapshot_invalid_iters)
        {
            if (itr->isActiveInMap())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is not active in map", itr->key.toView());
            updateDataSize(ERASE, itr->key.size, 0, itr->value.sizeInBytes(), /*remove_old=*/true);
            if (itr->getFreeKey())
                arena.free(const_cast<char *>(itr->key.data), itr->key.size);
            list.erase(itr);
        }
        snapshot_invalid_iters.clear();
    }

    void clear()
    {
        clearOutdatedNodes();
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

    void recalculateDataSize()
    {
        approximate_data_size = 0;
        for (auto & node : list)
        {
            approximate_data_size += node.key.size;
            approximate_data_size += node.value.sizeInBytes();
        }
    }

    uint64_t keyArenaSize() const { return 0; }

    iterator begin() { return list.begin(); }
    const_iterator begin() const { return list.cbegin(); }
    iterator end() { return list.end(); }
    const_iterator end() const { return list.cend(); }
};


}
