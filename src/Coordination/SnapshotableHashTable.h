#pragma once
#include <Common/HashTable/HashMap.h>
#include <Common/ArenaUtils.h>
#include <Common/logger_useful.h>
#include <algorithm>
#include <atomic>
#include <iterator>
#include <list>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template<typename V>
struct ListNode
{
    struct Tombstone
    {
        uint32_t free_key : 1;
        /// 0 = live; otherwise the version at which the element was superseded or erased.
        uint32_t invalidated_by : 31;
    };
    static_assert(std::is_trivially_copyable_v<Tombstone>);
    static_assert(std::atomic<Tombstone>::is_always_lock_free);

    static constexpr uint32_t MAX_VERSION = (1u << 31) - 1;

    std::string_view key;
    V value;

    /// Creation version; immutable once published.
    uint32_t version{0};
    std::atomic<Tombstone> tombstone{};

    ListNode(std::string_view key_, V value_)
        : key(key_)
        , value(std::move(value_))
    {
    }

    ListNode(ListNode && other) noexcept
        : key(other.key)
        , value(std::move(other.value))
        , version(other.version)
        , tombstone(other.tombstone.load(std::memory_order_relaxed))
    {
    }

    ListNode copyFromSnapshotNode()
    {
        return ListNode{key, value.copyFromSnapshotNode()};
    }

    bool isActiveInMap() const
    {
        return tombstone.load(std::memory_order_relaxed).invalidated_by == 0;
    }

    void invalidate(const uint32_t invalidated_by, const bool free_key)
    {
        Tombstone cur = tombstone.load(std::memory_order_relaxed);
        chassert(cur.invalidated_by == 0);
        chassert(invalidated_by != 0 && invalidated_by <= MAX_VERSION);
        cur.free_key = free_key;
        cur.invalidated_by = invalidated_by;
        tombstone.store(cur, std::memory_order_relaxed);
    }

    bool getFreeKey() const
    {
        return tombstone.load(std::memory_order_relaxed).free_key;
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
    using IndexMap = HashMap<std::string_view, Mapped>;

    List list;
    IndexMap map;
    /// Bumped once per issued read view.
    uint32_t current_version{0};
    /// Versions pinned by outstanding read views, ascending.
    std::vector<uint32_t> outstanding;
    /// Arena used for keys
    /// we don't use std::string because it uses 24 bytes (because of SSO)
    /// we want to always allocate the key on heap and use std::string_view to it
    GlobalArena arena;

    /// Superseded and erased elements, reclaimed once no view is outstanding.
    std::vector<Mapped> stale_nodes;

    std::atomic<uint64_t> approximate_data_size{0};

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
                approximate_data_size.fetch_add(key_size + value_size, std::memory_order_relaxed);
                if (remove_old && old_value_size != 0)
                    approximate_data_size.fetch_sub(key_size + old_value_size, std::memory_order_relaxed);
                break;
            case UPDATE:
                approximate_data_size.fetch_add(key_size + value_size, std::memory_order_relaxed);
                if (remove_old)
                    approximate_data_size.fetch_sub(key_size + old_value_size, std::memory_order_relaxed);
                break;
            case ERASE:
                if (remove_old)
                    approximate_data_size.fetch_sub(key_size + old_value_size, std::memory_order_relaxed);
                break;
            case CLEAR:
                approximate_data_size.store(0, std::memory_order_relaxed);
                break;
        }
    }

    bool mustCopyOnWrite(const ListElem & elem) const
    {
        return !outstanding.empty() && elem.version <= outstanding.back();
    }

    void insertOrReplace(std::string_view key, V value, bool owns_key)
    {
        size_t hash_value = map.hash(key);
        auto new_value_size = value.sizeInBytes();
        auto it = map.find(key, hash_value);
        uint64_t old_value_size = it == map.end() ? 0 : it->getMapped()->value.sizeInBytes();
        bool remove_old = true;

        if (it == map.end())
        {
            auto list_key = owns_key ? key : copyStringInArena(arena, key);
            ListElem elem{list_key, std::move(value)};
            elem.version = current_version;
            auto itr = list.insert(list.end(), std::move(elem));
            bool inserted = false;
            map.emplace(itr->key, it, inserted, hash_value);
            chassert(inserted);
            it->getMapped() = itr;
        }
        else
        {
            if (owns_key)
                arena.free(key.data(), key.size());

            auto list_itr = it->getMapped();
            if (mustCopyOnWrite(*list_itr))
            {
                ListElem elem{list_itr->key, std::move(value)};
                elem.version = current_version;
                chassert(current_version > outstanding.back());
                list_itr->invalidate(current_version, /*free_key=*/false);
                auto new_list_itr = list.insert(list.end(), std::move(elem));
                it->getMapped() = new_list_itr;
                stale_nodes.push_back(list_itr);

                remove_old = false;
            }
            else
            {
                list_itr->value = std::move(value);
            }
        }
        updateDataSize(INSERT_OR_REPLACE, key.size(), new_value_size, old_value_size, remove_old);
    }

public:

    using Node = V;
    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using ValueUpdater = std::function<void(V & value)>;

    /// Lock-free MVCC-style snapshot of the container.
    class ReadView
    {
    public:
        class Iterator
        {
        public:
            const ListElem & operator*() const
            {
                chassert(pos != prefix_size);
                return *it;
            }

            Iterator & operator++()
            {
                chassert(pos != prefix_size);
                advanceToVisibleOrEnd();
                return *this;
            }

            bool operator==(std::default_sentinel_t) const { return pos == prefix_size; }

        private:
            friend class ReadView;

            Iterator(const typename List::const_iterator it_, const size_t prefix_size_, const uint32_t view_version_, const size_t node_count_)
                : prefix_size(prefix_size_)
                , view_version(view_version_)
                , node_count(node_count_)
                , it(it_)
            {
                if (pos == prefix_size)
                    return;
                if (isNodeVisible(*it))
                    nodes_observed = 1;
                else
                    advanceToVisibleOrEnd();
            }

            bool isNodeVisible(const ListElem & node) const
            {
                chassert(node.version <= view_version);
                const auto tombstone = node.tombstone.load(std::memory_order_relaxed);
                return tombstone.invalidated_by == 0 || view_version < tombstone.invalidated_by;
            }

            void advanceToVisibleOrEnd()
            {
                while (++pos < prefix_size)
                {
                    ++it;
                    if (isNodeVisible(*it))
                    {
                        ++nodes_observed;
                        return;
                    }
                }
                chassert(nodes_observed == node_count);
            }

            const size_t prefix_size;
            const uint32_t view_version;
            const size_t node_count;

            typename List::const_iterator it;
            size_t pos = 0;
            size_t nodes_observed = 0;
        };

        ReadView(const ReadView &) = delete;

        Iterator begin() const { return Iterator{first, prefix_size, pinned_version, node_count}; }
        std::default_sentinel_t end() const { return {}; }

        size_t prefixSize() const { return prefix_size; }
        size_t nodeCount() const { return node_count; }
        uint32_t version() const { return pinned_version; }

    private:
        friend class SnapshotableHashTable;

        ReadView(const uint32_t pinned_version_, const size_t prefix_size_, const size_t node_count_, const typename List::const_iterator first_)
            : pinned_version(pinned_version_)
            , prefix_size(prefix_size_)
            , node_count(node_count_)
            , first(first_)
        {
        }

        const uint32_t pinned_version;
        const size_t prefix_size;
        const size_t node_count;
        const typename List::const_iterator first;
    };

    ~SnapshotableHashTable()
    {
        clear();
    }

    std::pair<typename IndexMap::LookupResult, bool> insert(const std::string & key, V value)
    {
        size_t hash_value = map.hash(key);
        auto it = map.find(key, hash_value);

        if (!it)
        {
            auto value_size = value.sizeInBytes();
            ListElem elem{copyStringInArena(arena, key), std::move(value)};
            elem.version = current_version;
            auto itr = list.insert(list.end(), std::move(elem));
            bool inserted = false;
            map.emplace(itr->key, it, inserted, hash_value);
            chassert(inserted);

            it->getMapped() = itr;
            updateDataSize(INSERT_OR_REPLACE, key.size(), value_size, 0);
            return std::make_pair(it, true);
        }

        return std::make_pair(it, false);
    }

    void reserve(size_t node_num) { map.reserve(node_num); }

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
        std::string_view key{key_data.release(), key_size};
        insertOrReplace(key, std::move(value), /*owns_key*/ true);
    }

    bool erase(const std::string & key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return false;

        bool remove_old = true;
        auto list_itr = it->getMapped();
        uint64_t old_data_size = list_itr->value.sizeInBytes();
        /// Note: while a read view is outstanding we can't deallocate the node even if
        /// `list_itr->version > outstanding.back()`. Because the node's key may be shared
        /// with another node (older version of this node). E.g. scenario:
        ///  1. Issue a read view.
        ///  2. updateValue(key, ...) - now `list` contains two nodes with `key` pointing to the
        ///     same range of memory.
        ///  3. erase(key) - can't do `arena.free(... list_itr->key ...)` as the key is still in
        ///     use by another node that is part of the view.
        if (!outstanding.empty())
        {
            chassert(current_version > outstanding.back());
            list_itr->invalidate(current_version, /*free_key=*/true);
            stale_nodes.push_back(list_itr);
            map.erase(it->getKey());

            remove_old = false;
        }
        else
        {
            map.erase(it->getKey());
            arena.free(const_cast<char *>(list_itr->key.data()), list_itr->key.size());
            list.erase(list_itr);
        }

        updateDataSize(ERASE, key.size(), 0, old_data_size, remove_old);
        return true;
    }

    bool contains(const std::string & key) const
    {
        return map.find(key) != map.end();
    }

    const_iterator updateValue(std::string_view key, ValueUpdater updater)
    {
        size_t hash_value = map.hash(key);
        auto it = map.find(key, hash_value);
        if (it == map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find key: '{}'", key);

        auto list_itr = it->getMapped();
        uint64_t old_value_size = list_itr->value.sizeInBytes();

        const_iterator ret;

        bool remove_old = true;
        if (mustCopyOnWrite(*list_itr))
        {
            auto elem_copy = list_itr->copyFromSnapshotNode();
            updateDataSize(UPDATE, key.size(), list_itr->value.sizeInBytes(), old_value_size, /*remove_old=*/true);
            chassert(current_version > outstanding.back());
            list_itr->invalidate(current_version, /*free_key=*/false);
            stale_nodes.push_back(list_itr);
            updater(elem_copy.value);

            elem_copy.version = current_version;
            auto itr = list.insert(list.end(), std::move(elem_copy));
            it->getMapped() = itr;
            ret = itr;

            remove_old = false;
        }
        else
        {
            updater(list_itr->value);
            ret = list_itr;
        }

        updateDataSize(UPDATE, key.size(), ret->value.sizeInBytes(), old_value_size, remove_old);
        return ret;
    }

    const_iterator find(std::string_view key) const
    {
        auto map_it = map.find(key);
        if (map_it != map.end())
            /// return std::make_shared<KVPair>(KVPair{map_it->getMapped()->key, map_it->getMapped()->value});
            return map_it->getMapped();
        return list.end();
    }


    const V & getValue(std::string_view key) const
    {
        auto it = map.find(key);
        if (it == map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find key: '{}'", key);
        return it->getMapped()->value;
    }

private:
    void clearStaleNodes() noexcept
    {
        chassert(outstanding.empty());

        for (auto & itr : stale_nodes)
        {
            chassert(!itr->isActiveInMap());
            updateDataSize(ERASE, itr->key.size(), 0, itr->value.sizeInBytes(), /*remove_old=*/true);
            if (itr->getFreeKey())
                arena.free(const_cast<char *>(itr->key.data()), itr->key.size());
            list.erase(itr);
        }
        stale_nodes.clear();
    }

public:
    void clear()
    {
        clearStaleNodes();
        map.clear();
        for (auto itr = list.begin(); itr != list.end(); ++itr)
            arena.free(const_cast<char *>(itr->key.data()), itr->key.size());
        list.clear();
        updateDataSize(CLEAR, 0, 0, 0);
    }

    std::unique_ptr<ReadView> issueReadView()
    {
        chassert(map.size() <= list.size());
        chassert(outstanding.empty() || current_version > outstanding.back());
        if (current_version == ListElem::MAX_VERSION)
        {
            LOG_ERROR(
                getLogger("SnapshotableHashTable"),
                "Read view version reached the maximum value. Terminating.");
            std::terminate();
        }

        const uint32_t pinned_version = current_version;
        ++current_version;
        outstanding.push_back(pinned_version);
        return std::unique_ptr<ReadView>(new ReadView(pinned_version, list.size(), map.size(), list.cbegin()));
    }

    void retireReadView(std::unique_ptr<ReadView> view) noexcept
    {
        chassert(view);
        auto it = std::find(outstanding.begin(), outstanding.end(), view->pinned_version);
        chassert(it != outstanding.end());
        outstanding.erase(it);

        if (outstanding.empty())
        {
            clearStaleNodes();
            chassert(map.size() == list.size());
        }
    }

    size_t size() const
    {
        return map.size();
    }

    size_t listSize() const
    {
        return list.size();
    }

    uint64_t getApproximateDataSize() const
    {
        return approximate_data_size.load(std::memory_order_relaxed);
    }

    void recalculateDataSize()
    {
        uint64_t data_size = 0;
        for (auto & node : list)
        {
            data_size += node.key.size();
            data_size += node.value.sizeInBytes();
        }
        approximate_data_size.store(data_size, std::memory_order_relaxed);
    }

    uint64_t keyArenaSize() const { return 0; }

    iterator begin() { return list.begin(); }
    const_iterator begin() const { return list.cbegin(); }
    iterator end() { return list.end(); }
    const_iterator end() const { return list.cend(); }
};


}
