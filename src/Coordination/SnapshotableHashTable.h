#pragma once
#include <common/StringRef.h>
#include <unordered_map>
#include <list>
#include <atomic>

namespace DB
{

template<typename V>
struct ListNode
{
    std::string key;
    V value;
    bool active_in_map;
};

template <class V>
class SnapshotableHashTable
{
private:

    using ListElem = ListNode<V>;
    using List = std::list<ListElem>;
    using IndexMap = std::unordered_map<StringRef, typename List::iterator, StringRefHash>;

    List list;
    IndexMap map;
    bool snapshot_mode{false};

public:

    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using reverse_iterator = typename List::reverse_iterator;
    using const_reverse_iterator = typename List::const_reverse_iterator;
    using ValueUpdater = std::function<void(V & value)>;

    bool insert(const std::string & key, const V & value)
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            ListElem elem{key, value, true};
            auto itr = list.insert(list.end(), elem);
            map.emplace(itr->key, itr);
            return true;
        }

        return false;
    }


    void insertOrReplace(const std::string & key, const V & value)
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            ListElem elem{key, value, true};
            auto itr = list.insert(list.end(), elem);
            map.emplace(itr->key, itr);
        }
        else
        {
            auto list_itr = it->second;
            if (snapshot_mode)
            {
                ListElem elem{key, value, true};
                list_itr->active_in_map = false;
                auto new_list_itr = list.insert(list.end(), elem);
                map.erase(it);
                map.emplace(new_list_itr->key, new_list_itr);
            }
            else
            {
                list_itr->value = value;
            }
        }
    }

    bool erase(const std::string & key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return false;

        auto list_itr = it->second;
        if (snapshot_mode)
        {
            list_itr->active_in_map = false;
            map.erase(it);
        }
        else
        {
            map.erase(it);
            list.erase(list_itr);
        }

        return true;
    }

    bool contains(const std::string & key) const
    {
        return map.find(key) != map.end();
    }

    const_iterator updateValue(const std::string & key, ValueUpdater updater)
    {
        auto it = map.find(key);
        assert(it != map.end());
        if (snapshot_mode)
        {
            auto list_itr = it->second;
            auto elem_copy = *(list_itr);
            list_itr->active_in_map = false;
            map.erase(it);
            updater(elem_copy.value);
            auto itr = list.insert(list.end(), elem_copy);
            map.emplace(itr->key, itr);
            return itr;
        }
        else
        {
            auto list_itr = it->second;
            updater(list_itr->value);
            return list_itr;
        }
    }

    const_iterator find(const std::string & key) const
    {
        auto map_it = map.find(key);
        if (map_it != map.end())
            return map_it->second;
        return list.end();
    }

    const V & getValue(const std::string & key) const
    {
        auto it = map.find(key);
        assert(it != map.end());
        return it->second->value;
    }

    void clearOutdatedNodes()
    {
        auto start = list.begin();
        auto end = list.end();
        for (auto itr = start; itr != end;)
        {
            if (!itr->active_in_map)
                itr = list.erase(itr);
            else
                itr++;
        }
    }

    void clear()
    {
        list.clear();
        map.clear();
    }

    void enableSnapshotMode()
    {
        snapshot_mode = true;
    }

    void disableSnapshotMode()
    {
        snapshot_mode = false;
    }

    size_t size() const
    {
        return map.size();
    }

    size_t snapshotSize() const
    {
        return list.size();
    }


    iterator begin() { return list.begin(); }
    const_iterator begin() const { return list.cbegin(); }
    iterator end() { return list.end(); }
    const_iterator end() const { return list.cend(); }

    reverse_iterator rbegin() { return list.rbegin(); }
    const_reverse_iterator rbegin() const { return list.crbegin(); }
    reverse_iterator rend() { return list.rend(); }
    const_reverse_iterator rend() const { return list.crend(); }
};


}
