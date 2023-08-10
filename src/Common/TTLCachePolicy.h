#pragma once

#include <Common/ICachePolicy.h>

#include <unordered_map>

namespace DB
{

/// TTLCachePolicy evicts entries for which IsStaleFunction returns true.
/// The cache size (in bytes and number of entries) can be changed at runtime. It is expected to set both sizes explicitly after construction.
template <typename Key, typename Mapped, typename HashFunction, typename WeightFunction, typename IsStaleFunction>
class TTLCachePolicy : public ICachePolicy<Key, Mapped, HashFunction, WeightFunction>
{
public:
    using Base = ICachePolicy<Key, Mapped, HashFunction, WeightFunction>;
    using typename Base::MappedPtr;
    using typename Base::KeyMapped;
    using typename Base::OnWeightLossFunction;

    TTLCachePolicy()
        : max_size_in_bytes(0)
        , max_count(0)
    {
    }

    size_t weight(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return size_in_bytes;
    }

    size_t count(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return cache.size();
    }

    size_t maxSize(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return max_size_in_bytes;
    }

    void setMaxCount(size_t max_count_, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        /// lazy behavior: the cache only shrinks upon the next insert
        max_count = max_count_;
    }

    void setMaxSize(size_t max_size_in_bytes_, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        /// lazy behavior: the cache only shrinks upon the next insert
        max_size_in_bytes = max_size_in_bytes_;
    }

    void reset(std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        cache.clear();
    }

    void remove(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        auto it = cache.find(key);
        if (it == cache.end())
            return;
        size_in_bytes -= weight_function(*it->second);
        cache.erase(it);
    }

    MappedPtr get(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        auto it = cache.find(key);
        if (it == cache.end())
            return {};
        return it->second;
    }

    std::optional<KeyMapped> getWithKey(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        auto it = cache.find(key);
        if (it == cache.end())
            return std::nullopt;
        return std::make_optional<KeyMapped>({it->first, it->second});
    }

    /// Evicts on a best-effort basis. If there are too many non-stale entries, the new entry may not be cached at all!
    void set(const Key & key, const MappedPtr & mapped, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        chassert(mapped.get());

        const size_t entry_size_in_bytes = weight_function(*mapped);

        auto sufficient_space_in_cache = [&]()
        {
            return (size_in_bytes + entry_size_in_bytes <= max_size_in_bytes) && (cache.size() + 1 <= max_count);
        };

        if (!sufficient_space_in_cache())
        {
            /// Remove stale entries
            for (auto it = cache.begin(); it != cache.end();)
                if (is_stale_function(it->first))
                {
                    size_in_bytes -= weight_function(*it->second);
                    it = cache.erase(it);
                }
                else
                    ++it;
        }

        if (sufficient_space_in_cache())
        {
            /// Insert or replace key
            if (auto it = cache.find(key); it != cache.end())
            {
                size_in_bytes -= weight_function(*it->second);
                cache.erase(it); // stupid bug: (*) doesn't replace existing entries (likely due to custom hash function), need to erase explicitly
            }

            cache[key] = std::move(mapped); // (*)
            size_in_bytes += entry_size_in_bytes;
        }
    }

    std::vector<KeyMapped> dump() const override
    {
        std::vector<KeyMapped> res;
        for (const auto & [key, mapped] : cache)
            res.push_back({key, mapped});
        return res;
    }

private:
    using Cache = std::unordered_map<Key, MappedPtr, HashFunction>;
    Cache cache;

    /// TODO To speed up removal of stale entries, we could also add another container sorted on expiry times which maps keys to iterators
    /// into the cache. To insert an entry, add it to the cache + add the iterator to the sorted container. To remove stale entries, do a
    /// binary search on the sorted container and erase all left of the found key.

    size_t size_in_bytes = 0;
    size_t max_size_in_bytes;
    size_t max_count;

    WeightFunction weight_function;
    IsStaleFunction is_stale_function;
    /// TODO support OnWeightLossFunction callback
};

}
