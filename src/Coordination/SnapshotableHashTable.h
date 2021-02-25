#pragma once
#include <common/StringRef.h>
#include <unordered_map>
#include <list>

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
    bool snapshot_mode;

public:

    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using reverse_iterator = typename List::reverse_iterator;
    using const_reverse_iterator = typename List::const_reverse_iterator;
    using ValueUpdater = std::function<void(V & value)>;

    bool insert(const std::string & key, V && value)
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            ListElem elem{key, value, true};
            auto itr = list.insert(list.end(), elem);
            map.emplace(itr->key, itr);
            return true;
        }
        else
        {
            ListElem elem{key, value, true};
            if (snapshot_mode)
            {
                auto list_itr = it->second;
                list_itr->active_in_map = false;
                auto new_list_itr = list.insert(list.end(), elem);
                map[new_list_itr->key] = new_list_itr;
            }
            else
            {
                list.erase(it->second);
                auto itr = list.insert(list.end(), elem);
                map[itr->key] = itr;
            }
            return false;
        }
    }

    bool contains(const std::string & key) const
    {
        return map.find(key) != map.end();
    }

    void updateValue(const std::string & key, ValueUpdater updater)
    {
        auto it = map.find(key);
        assert(it != map.end());
        if (snapshot_mode)
        {
            auto list_itr = it->second;
            list_itr->active_in_map = false;
            auto elem_copy = *(list_itr);
            updater(elem_copy.value);
            auto itr = list.insert(list.end(), elem_copy);

            map[itr->key] = itr;
        }
        else
        {
            auto list_itr = it->second;
            updater(list_itr->value);
        }
    }

    const V & getValue(const std::string & key) const
    {
        auto it = map.find(key);
        assert(it != map.end());
        return it->second->value;
    }

    void clear()
    {
        list.clear();
        map.clear();
    }

    bool enableSnapshotMode()
    {
        bool old = snapshot_mode;
        snapshot_mode = true;
        return old;
    }

    bool disableSnapshotMode()
    {
        bool old = snapshot_mode;
        snapshot_mode = false;
        return old;
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
