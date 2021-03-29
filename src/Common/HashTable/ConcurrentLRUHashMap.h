#pragma once

#include <shared_mutex>
#include <unordered_map>

#include <Common/HashTable/Hash.h>

struct StandardMutexProvider
{

    using SharedMutexType = std::shared_mutex;

    using WriteLockType = std::unique_lock<std::shared_mutex>;

    using ReadLockType = std::shared_lock<std::shared_mutex>;

    std::shared_mutex & getSharedMutex() { return mutex; }

private:
    std::shared_mutex mutex;
};

struct DummyMutexProvider
{
    struct DummySharedMutexType {};

    struct DummyWriteLockType {
        explicit DummyWriteLockType(DummySharedMutexType &) {}
    };

    struct DummyReadLockType {
        explicit DummyReadLockType(DummySharedMutexType &) {}
    };

    using SharedMutexType = DummySharedMutexType;
    using WriteLockType = DummyWriteLockType;
    using ReadLockType = DummyReadLockType;

    DummySharedMutexType & getSharedMutex() { return mutex; }

private:
    DummySharedMutexType mutex;
};

template <typename TKey, typename TValue, typename THash, typename TSharedMutexProvider>
class ConcurrentLRUHashMapImpl
{
public:
    using Key = TKey;
    using Value = TValue;
    using Hash = THash;

    using key_type = Key;
    using value_type = Value;

    explicit ConcurrentLRUHashMapImpl(size_t max_size_)
        : max_size(max_size_)
        , epoch(max_size)
    {}

    /// TODO: std insert return value
    void insert(const Key & key, const Value & value)
    {
        return emplace(key, value);
    }

    void insert(const Key & key, Value && value)
    {
        return emplace(key, std::move(value));
    }

    template <typename ...Args>
    void emplace(const Key & key, Args && ... args)
    {
        WriteLockType scoped_write_lock(shared_mutex_provider.getSharedMutex());

        auto it = map.find(key);

        if (it == map.end())
        {
            if (unlikely(map.size() == max_size * 2))
                eraseOldCells(scoped_write_lock);

            auto emplace_result = map.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(key),
                std::forward_as_tuple(std::forward<Args>(args)...));

            auto emplace_it = emplace_result.first;
            assert(emplace_result.second);

            size_t previous_epoch = epoch.fetch_add(1, std::memory_order_relaxed);
            emplace_it->second.epoch.store(previous_epoch, std::memory_order_relaxed);
        }
        else
        {
            auto & value_to_update = it->second;

            value_to_update.value = Value(std::forward<Args>(args)...);

            holes_count.fetch_add(1, std::memory_order_relaxed);
            size_t previous_epoch = epoch.fetch_add(1, std::memory_order_relaxed);
            value_to_update.epoch.store(previous_epoch, std::memory_order_relaxed);
        }
    }

    Value * find(const Key & key)
    {
        ReadLockType scoped_read_lock(shared_mutex_provider.getSharedMutex());

        auto iterator_in_map = map.find(key);

        if (iterator_in_map == map.end())
           return nullptr;

        auto & value_to_update = iterator_in_map->second;

        /// TODO: This place is tricky for concurrency

        size_t epoch_value = epoch.load();
        size_t holes_count_value = holes_count.load();

        size_t last_valid_epoch = epoch_value - holes_count_value - max_size;

        if (value_to_update.epoch < last_valid_epoch)
            return nullptr;

        holes_count.fetch_add(1, std::memory_order_relaxed);

        size_t epoch_previous_value = epoch.fetch_add(1);
        value_to_update.epoch = epoch_previous_value;

        return &iterator_in_map->second.value;
    }

    const Value * find(const Value & key) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key);
    }

    template <typename ForEachFunc>
    void forEach(ForEachFunc && func) const
    {
        WriteLockType write_lock(shared_mutex_provider.getSharedMutex());

        size_t epoch_value = epoch.load();
        size_t holes_count_value = holes_count.load();

        for (auto & [key, value] : map)
        {
            size_t last_valid_epoch = epoch_value - holes_count_value - max_size;

            if (value.epoch < last_valid_epoch)
                continue;

            std::forward<ForEachFunc>(func)(key, value.value);
        }
    }

    size_t getMaxSize() const
    {
        ReadLockType lock(shared_mutex_provider.getSharedMutex());
        return max_size;
    }

    size_t size() const
    {
        /// This size is broken it can be greater than cache size by factor of 2
        /// but to calculate size with valid cells it will be O(n)
        ReadLockType lock(shared_mutex_provider.getSharedMutex());
        return map.size();
    }

    bool empty() const
    {
        ReadLockType lock(shared_mutex_provider.getSharedMutex());
        return map.empty();
    }

    bool contains(const Key & key)
    {
        return find(key) != nullptr;
    }

    void clear()
    {
        WriteLockType lock(shared_mutex_provider.getSharedMutex());
        map.clear();
    }

private:
    using SharedMutexProvider = TSharedMutexProvider;
    using WriteLockType = typename SharedMutexProvider::WriteLockType;
    using ReadLockType = typename SharedMutexProvider::ReadLockType;

    void eraseOldCells(WriteLockType &)
    {
        size_t epoch_value = epoch.load();
        size_t holes_count_value = holes_count.load();

        for (auto it = map.begin(); it != map.end();)
        {
            const auto & value = it->second;
            size_t last_valid_epoch = epoch_value - holes_count_value - max_size;

            if (value.epoch < last_valid_epoch)
                it = map.erase(it);
            else
                ++it;
        }
    }

    struct Node
    {
        explicit Node(Value value_) : value(std::move(value_)) {}

        Value value;

        std::atomic<size_t> epoch = 0;
    };

    size_t max_size;
    std::atomic<size_t> epoch;

    /// TODO: Abseil or our hash table
    std::unordered_map<Key, Node> map;
    std::atomic<size_t> holes_count = 0;

    mutable SharedMutexProvider shared_mutex_provider {};
};

template <typename Key, typename Value, typename Hash = DefaultHash<Key>>
using ConcurrentLRUHashMap = ConcurrentLRUHashMapImpl<Key, Value, Hash, StandardMutexProvider>;

template <typename Key, typename Value, typename Hash = DefaultHash<Key>>
using ConcurrentLRUHashMapNoMutex = ConcurrentLRUHashMapImpl<Key, Value, Hash, DummyMutexProvider>;
