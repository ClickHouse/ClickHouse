#pragma once

#include <Common/ICachePolicy.h>
#include <base/UUID.h>

#include <limits>
#include <unordered_map>

namespace DB
{

class PerUserTTLCachePolicyUserQuota : public ICachePolicyUserQuota
{
public:
    void setQuotaForUser(const UUID & user_id, size_t max_size_in_bytes, size_t max_entries) override
    {
        quotas[user_id] = {max_size_in_bytes, max_entries};
    }

    void increaseActual(const UUID & user_id, size_t entry_size_in_bytes) override
    {
        auto & actual_for_user = actual[user_id];
        actual_for_user.size_in_bytes += entry_size_in_bytes;
        actual_for_user.num_items += 1;
    }

    void decreaseActual(const UUID & user_id, size_t entry_size_in_bytes) override
    {
        chassert(actual.contains(user_id));

        chassert(actual[user_id].size_in_bytes >= entry_size_in_bytes);
        actual[user_id].size_in_bytes -= entry_size_in_bytes;

        chassert(actual[user_id].num_items >= 1);
        actual[user_id].num_items -= 1;
    }

    bool approveWrite(const UUID & user_id, size_t entry_size_in_bytes) const override
    {
        auto it_actual = actual.find(user_id);
        Resources actual_for_user{.size_in_bytes = 0, .num_items = 0}; /// if no user is found, the default is no resource consumption
        if (it_actual != actual.end())
            actual_for_user = it_actual->second;

        auto it_quota = quotas.find(user_id);
        Resources quota_for_user{.size_in_bytes = std::numeric_limits<size_t>::max(), .num_items = std::numeric_limits<size_t>::max()}; /// if no user is found, the default is no threshold
        if (it_quota != quotas.end())
            quota_for_user = it_quota->second;

        /// Special case: A quota configured as 0 means no threshold
        if (quota_for_user.size_in_bytes == 0)
            quota_for_user.size_in_bytes = std::numeric_limits<UInt64>::max();
        if (quota_for_user.num_items == 0)
            quota_for_user.num_items = std::numeric_limits<UInt64>::max();

        /// Check size quota
        if (actual_for_user.size_in_bytes + entry_size_in_bytes > quota_for_user.size_in_bytes)
            return false;

        /// Check items quota
        if (actual_for_user.num_items + 1 > quota_for_user.num_items)
            return false;

        return true;
    }

    void clear() override
    {
        actual.clear();
    }

    struct Resources
    {
        size_t size_in_bytes = 0;
        size_t num_items = 0;
    };

    /// user id --> cache size quota (in bytes) / number of items quota
    std::map<UUID, Resources> quotas;
    /// user id --> actual cache usage (in bytes) / number of items
    std::map<UUID, Resources> actual;
};


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

    explicit TTLCachePolicy(CachePolicyUserQuotaPtr quotas_)
        : Base(std::move(quotas_))
        , max_size_in_bytes(0)
        , max_count(0)
    {
    }

    size_t sizeInBytes() const override
    {
        return size_in_bytes;
    }

    size_t count() const override
    {
        return cache.size();
    }

    size_t maxSizeInBytes() const override
    {
        return max_size_in_bytes;
    }

    void setMaxCount(size_t max_count_) override
    {
        /// lazy behavior: the cache only shrinks upon the next insert
        max_count = max_count_;
    }

    void setMaxSizeInBytes(size_t max_size_in_bytes_) override
    {
        /// lazy behavior: the cache only shrinks upon the next insert
        max_size_in_bytes = max_size_in_bytes_;
    }

    void clear() override
    {
        cache.clear();
        Base::user_quotas->clear();
    }

    void remove(const Key & key) override
    {
        auto it = cache.find(key);
        if (it == cache.end())
            return;
        size_t sz = weight_function(*it->second);
        if (it->first.user_id.has_value())
            Base::user_quotas->decreaseActual(*it->first.user_id, sz);
        cache.erase(it);
        size_in_bytes -= sz;
    }

    void remove(std::function<bool(const Key &, const MappedPtr &)> predicate) override
    {
        for (auto it = cache.begin(); it != cache.end();)
        {
            if (predicate(it->first, it->second))
            {
                size_t sz = weight_function(*it->second);
                if (it->first.user_id.has_value())
                    Base::user_quotas->decreaseActual(*it->first.user_id, sz);
                it = cache.erase(it);
                size_in_bytes -= sz;
            }
            else
                ++it;
        }
    }

    MappedPtr get(const Key & key) override
    {
        auto it = cache.find(key);
        if (it == cache.end())
            return {};
        return it->second;
    }

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        auto it = cache.find(key);
        if (it == cache.end())
            return std::nullopt;
        return std::make_optional<KeyMapped>({it->first, it->second});
    }

    /// Evicts on a best-effort basis. If there are too many non-stale entries, the new entry may not be cached at all!
    void set(const Key & key, const MappedPtr & mapped) override
    {
        chassert(mapped.get());

        const size_t entry_size_in_bytes = weight_function(*mapped);

        /// Checks against per-cache limits
        auto sufficient_space_in_cache = [&]()
        {
            return (size_in_bytes + entry_size_in_bytes <= max_size_in_bytes) && (cache.size() + 1 <= max_count);
        };

        /// Checks against per-user limits
        auto sufficient_space_in_cache_for_user = [&]()
        {
            if (key.user_id.has_value())
                return Base::user_quotas->approveWrite(*key.user_id, entry_size_in_bytes);
            return true;
        };

        if (!sufficient_space_in_cache() || !sufficient_space_in_cache_for_user())
        {
            /// Remove stale entries
            for (auto it = cache.begin(); it != cache.end();)
                if (is_stale_function(it->first))
                {
                    size_t sz = weight_function(*it->second);
                    if (it->first.user_id.has_value())
                        Base::user_quotas->decreaseActual(*it->first.user_id, sz);
                    it = cache.erase(it);
                    size_in_bytes -= sz;
                }
                else
                    ++it;
        }

        if (sufficient_space_in_cache() && sufficient_space_in_cache_for_user())
        {
            /// Insert or replace key
            if (auto it = cache.find(key); it != cache.end())
            {
                size_t sz = weight_function(*it->second);
                if (it->first.user_id.has_value())
                    Base::user_quotas->decreaseActual(*it->first.user_id, sz);
                cache.erase(it); // stupid bug: (*) doesn't replace existing entries (likely due to custom hash function), need to erase explicitly
                size_in_bytes -= sz;
            }

            cache[key] = std::move(mapped); // (*)
            size_in_bytes += entry_size_in_bytes;
            if (key.user_id.has_value())
                Base::user_quotas->increaseActual(*key.user_id, entry_size_in_bytes);
        }
    }

    std::vector<KeyMapped> dump() const override
    {
        std::vector<KeyMapped> res;
        res.reserve(cache.size());
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
