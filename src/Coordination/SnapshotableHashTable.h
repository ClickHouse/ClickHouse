#pragma once
#include <limits>
#include <iostream>
#include <base/StringRef.h>
#include <memory>
#include <unordered_map>
#include <list>
#include <atomic>
#include <Common/SipHash.h>
#include <Common/HashTable/robin_hood.h>
#include <Common/HashTable/IncrementalRehashTable.h>
#include <Common/ArenaWithFreeLists.h>

namespace DB
{
#if 1
template <typename K, typename V, typename Hash, int N=8>
class ArrayMap: protected Hash
{
    //using undermap = std::unordered_map<K, V, Hash>;
    using Us = robin_hood::unordered_map<K, V, Hash>;

    static constexpr bool is_flat = true;
    static constexpr bool is_set = false;
    using key_type = K;
    using mapped_type = V;
    using value_type = typename std::conditional<
        is_set, K,
        robin_hood::pair<typename std::conditional<is_flat, K, K const>::type, V>>::type;
    using size_type = size_t;
    using Self = ArrayMap<K, V, Hash, N>;
    using inner_iterator = typename Us::iterator;
public:
    size_t hash(const K & x) const { return Hash::operator()(x); }
    template <bool IsConst>
    struct Iter
    {
    public:
        using UsPtr = typename std::conditional<IsConst, Self const*, Self*>::type;
        using difference_type = std::ptrdiff_t;
        using value_type = typename Self::value_type;
        using reference = typename std::conditional<IsConst, value_type const&, value_type&>::type;
        using pointer = typename std::conditional<IsConst, value_type const*, value_type*>::type;
        using iterator_category = std::forward_iterator_tag;
        using inner_iter = typename std::conditional<IsConst, typename Us::const_iterator, typename Us::iterator>::type;

        Iter() = default;
        Iter(UsPtr ptr, inner_iter iter, int idx) : ref(ptr), m_iter(iter), index(idx) {}

        reference operator*() const { return *m_iter; }
        pointer operator->() { return &*m_iter; }
        Iter& operator++() noexcept
        {
            /*
            ++m_iter;
            if (index == 0 && ref->rehashing && m_iter == ref->store[0]->end())
            {
                m_iter = ref->store[1]->begin();
            }
            */
            return *this;
        }
        Iter operator++(int) noexcept { Iter tmp = *this; ++(*this); return tmp; }
        template <bool O>
        bool operator==(Iter<O> const& o) const noexcept
        {
            return m_iter == o.m_iter && index == o.index && ref == o.ref;
        }
        template <bool O>
        bool operator!=(Iter<O> const& o) const noexcept
        {
            return m_iter != o.m_iter;
        }
        UsPtr ref;
        inner_iter m_iter;
        int index{0};
    };
    using iterator = Iter<false>;
    using const_iterator = Iter<true>;
    iterator find(K const & key)
    {
        auto index = hash(key)&(N-1);
        auto iter = store[index].find(key);
        //std::cout << "find " << iter->first << " value " << std::endl;
        if (iter == store[index].end())
            return end();
        return iterator(this, iter, index);
    }
    const_iterator find(K const & key) const
    {
        auto index = hash(key)&(N-1);
        auto iter = store[index].find(key);
        if (iter == store[index].end())
            return end();
        //std::cout << "find " << iter->first << " value " << std::endl;
        return const_iterator(this, iter, index);
    }
    iterator begin()
    {
        auto iter = store[0].begin();
        return iterator(this, iter, 0);
    }
    const_iterator begin() const
    {
        auto iter = store[0].begin();
        return const_iterator(this, iter, 0);
    }
    const_iterator end() const
    {
        auto iter = store[N-1].end();
        return const_iterator(this, iter, N-1);
    }
    iterator end()
    {
        auto iter = store[N-1].end();
        return iterator(this, iter, N-1);
    }
    bool getValue(const K & key, V & value) const
    {
        auto index = hash(key)&(N-1);
        auto iter = store[index].find(key);
        if (iter != store[index].end())
        {
            value = iter->second;
            return true;
        }
        return false;
    }
    bool exists(const K & key) const
    {
        auto index = hash(key)&(N-1);
        //std::cout << "key " << key << ", index " << index << std::endl;
        return store[index].contains(key);
    }
    void erase(iterator it)
    {
        store[it.index].erase(it.m_iter);
    }
    void erase(const K & key)
    {
        auto index = hash(key)&(N-1);
        store[index].erase(key);
    }
    void emplace(const K & key, const V & value)
    {
        auto index = hash(key)&(N-1);
        //std::cout << "key " << key << ", index " << index << std::endl;
        /*auto it = */store[index].emplace(key, value);
        //std::cout << "insert res: " << it.second << std::endl;
    }
    void clear() noexcept
    {   
        for (int i = 0; i < N; ++i)
        {
            store[i].clear();
        }
    }
    void reserve(size_t n)
    {
        for (int i = 0; i < N; ++i)
        {
            store[i].reserve(n);
        }
    }
    size_t size() const
    {
        size_t s = 0;
        for (int i = 0; i < N; ++i)
        {
            s += store[i].size();
        }
        return s;
    }

private:
    Us store[N];
};
#endif

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
    //using IndexMap = std::unordered_map<StringRef, typename List::iterator, StringRefHash>;
    //using IndexMap = ArrayMap<StringRef, typename List::iterator, StringRefHash>;
    using IndexMap = my_unordered_map<std::string, typename List::iterator/*, StringRefHash*/>;
    //using IndexMap = TwoLevelHashMap<StringRef, typename List::iterator>;

    List list;
    IndexMap map;
    bool snapshot_mode{false};

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
                        approximate_data_size += key_size;
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
    using reverse_iterator = typename List::reverse_iterator;
    using const_reverse_iterator = typename List::const_reverse_iterator;
    using ValueUpdater = std::function<void(V & value)>;

    explicit SnapshotableHashTable(size_t n = 1000000)
    {
        map.reserve(n);
    }
    bool insert(const std::string & key, const V & value)
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            ListElem elem{key, value, true};
            auto itr = list.insert(list.end(), elem);
            map.emplace(itr->key, itr);
            updateDataSize(INSERT, key.size(), value.sizeInBytes(), 0);
            return true;
        }

        return false;
    }


    void insertOrReplace(const std::string & key, const V & value)
    {
        auto it = map.find(key);
        uint64_t old_value_size = it == map.end() ? 0 : it->second->value.sizeInBytes();

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
        updateDataSize(INSERT_OR_REPLACE, key.size(), value.sizeInBytes(), old_value_size);
    }

    bool erase(const std::string & key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return false;

        auto list_itr = it->second;
        uint64_t old_data_size = list_itr->value.sizeInBytes();
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

        updateDataSize(ERASE, key.size(), 0, old_data_size);
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

        auto list_itr = it->second;
        uint64_t old_value_size = list_itr->value.sizeInBytes();

        const_iterator ret;

        if (snapshot_mode)
        {
            auto elem_copy = *(list_itr);
            list_itr->active_in_map = false;
            map.erase(it);
            updater(elem_copy.value);
            auto itr = list.insert(list.end(), elem_copy);
            map.emplace(itr->key, itr);
            ret = itr;
        }
        else
        {
            updater(list_itr->value);
            ret = list_itr;
        }
        updateDataSize(UPDATE_VALUE, key.size(), ret->value.sizeInBytes(), old_value_size);
        return ret;
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
            {
                updateDataSize(CLEAR_OUTDATED_NODES, itr->key.size(), itr->value.sizeInBytes(), 0);
                itr = list.erase(itr);
            }
            else
                itr++;
        }
    }

    void clear()
    {
        list.clear();
        map.clear();
        updateDataSize(CLEAR, 0, 0, 0);
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

    uint64_t getApproximateDataSize() const
    {
        return approximate_data_size;
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
