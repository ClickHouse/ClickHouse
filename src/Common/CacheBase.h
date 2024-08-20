#pragma once

#include <Common/Exception.h>
#include <Common/ICachePolicy.h>
#include <Common/LRUCachePolicy.h>
#include <Common/SLRUCachePolicy.h>

#include <base/UUID.h>
#include <base/defines.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Thread-safe cache that evicts entries using special cache policy
/// (default policy evicts entries which are not used for a long time).
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size_in_bytes.
/// Value weight should not change after insertion.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = EqualWeightFunction<TMapped>>
class CacheBase
{
private:
    using CachePolicy = ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>;

public:
    using Key = typename CachePolicy::Key;
    using Mapped = typename CachePolicy::Mapped;
    using MappedPtr = typename CachePolicy::MappedPtr;
    using KeyMapped = typename CachePolicy::KeyMapped;

    static constexpr auto NO_MAX_COUNT = 0uz;
    static constexpr auto DEFAULT_SIZE_RATIO = 0.5l;

    /// Use this ctor if you only care about the cache size but not internals like the cache policy.
    explicit CacheBase(size_t max_size_in_bytes, size_t max_count = NO_MAX_COUNT, double size_ratio = DEFAULT_SIZE_RATIO)
        : CacheBase("SLRU", max_size_in_bytes, max_count, size_ratio)
    {
    }

    /// Use this ctor if the user should be able to configure the cache policy and cache sizes via settings. Supports only general-purpose policies LRU and SLRU.
    explicit CacheBase(std::string_view cache_policy_name, size_t max_size_in_bytes, size_t max_count, double size_ratio)
    {
        auto on_weight_loss_function = [&](size_t weight_loss) { onRemoveOverflowWeightLoss(weight_loss); };

        if (cache_policy_name.empty())
        {
            static constexpr auto default_cache_policy = "SLRU";
            cache_policy_name = default_cache_policy;
        }

        if (cache_policy_name == "LRU")
        {
            using LRUPolicy = LRUCachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
            cache_policy = std::make_unique<LRUPolicy>(max_size_in_bytes, max_count, on_weight_loss_function);
        }
        else if (cache_policy_name == "SLRU")
        {
            using SLRUPolicy = SLRUCachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
            cache_policy = std::make_unique<SLRUPolicy>(max_size_in_bytes, max_count, size_ratio, on_weight_loss_function);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown cache policy name: {}", cache_policy_name);
    }

    /// Use this ctor to provide an arbitrary cache policy.
    explicit CacheBase(std::unique_ptr<ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>> cache_policy_)
        : cache_policy(std::move(cache_policy_))
    {}

    MappedPtr get(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto res = cache_policy->get(key);
        if (res)
            ++hits;
        else
            ++misses;
        return res;
    }

    std::optional<KeyMapped> getWithKey(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto res = cache_policy->getWithKey(key);
        if (res.has_value())
            ++hits;
        else
            ++misses;
        return res;
    }

    void set(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);
        cache_policy->set(key, mapped);
    }

    /// If the value for the key is in the cache, returns it. If it is not, calls load_func() to
    /// produce it, saves the result in the cache and returns it.
    /// Only one of several concurrent threads calling getOrSet() will call load_func(),
    /// others will wait for that call to complete and will use its result (this helps prevent cache stampede).
    /// Exceptions occurring in load_func will be propagated to the caller. Another thread from the
    /// set of concurrent threads will then try to call its load_func etc.
    ///
    /// Returns std::pair of the cached value and a bool indicating whether the value was produced during this call.
    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        InsertTokenHolder token_holder;
        {
            std::lock_guard cache_lock(mutex);
            auto val = cache_policy->get(key);
            if (val)
            {
                ++hits;
                return std::make_pair(val, false);
            }

            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);

            token_holder.acquire(&key, token, cache_lock);
        }

        InsertToken * token = token_holder.token.get();

        std::lock_guard token_lock(token->mutex);

        token_holder.cleaned_up = token->cleaned_up;

        if (token->value)
        {
            /// Another thread already produced the value while we waited for token->mutex.
            ++hits;
            return std::make_pair(token->value, false);
        }

        ++misses;
        token->value = load_func();

        std::lock_guard cache_lock(mutex);

        /// Insert the new value only if the token is still in present in insert_tokens.
        /// (The token may be absent because of a concurrent clear() call).
        bool result = false;
        auto token_it = insert_tokens.find(key);
        if (token_it != insert_tokens.end() && token_it->second.get() == token)
        {
            cache_policy->set(key, token->value);
            result = true;
        }

        if (!token->cleaned_up)
            token_holder.cleanup(token_lock, cache_lock);

        return std::make_pair(token->value, result);
    }

    void getStats(size_t & out_hits, size_t & out_misses) const
    {
        std::lock_guard lock(mutex);
        out_hits = hits;
        out_misses = misses;
    }

    std::vector<KeyMapped> dump() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->dump();
    }

    void clear()
    {
        std::lock_guard lock(mutex);
        insert_tokens.clear();
        hits = 0;
        misses = 0;
        cache_policy->clear();
    }

    void remove(const Key & key)
    {
        std::lock_guard lock(mutex);
        cache_policy->remove(key);
    }

    void remove(std::function<bool(const Key&, const MappedPtr &)> predicate)
    {
        std::lock_guard lock(mutex);
        cache_policy->remove(predicate);
    }

    size_t sizeInBytes() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->sizeInBytes();
    }

    size_t count() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->count();
    }

    size_t maxSizeInBytes() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->maxSizeInBytes();
    }

    void setMaxCount(size_t max_count)
    {
        std::lock_guard lock(mutex);
        cache_policy->setMaxCount(max_count);
    }

    void setMaxSizeInBytes(size_t max_size_in_bytes)
    {
        std::lock_guard lock(mutex);
        cache_policy->setMaxSizeInBytes(max_size_in_bytes);
    }

    void setQuotaForUser(const UUID & user_id, size_t max_size_in_bytes, size_t max_entries)
    {
        std::lock_guard lock(mutex);
        cache_policy->setQuotaForUser(user_id, max_size_in_bytes, max_entries);
    }

    virtual ~CacheBase() = default;

protected:
    mutable std::mutex mutex;

private:
    std::unique_ptr<CachePolicy> cache_policy TSA_GUARDED_BY(mutex);

    std::atomic<size_t> hits{0};
    std::atomic<size_t> misses{0};

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(CacheBase & cache_) : cache(cache_) {}

        std::mutex mutex;
        bool cleaned_up TSA_GUARDED_BY(mutex) = false;
        MappedPtr value TSA_GUARDED_BY(mutex);

        CacheBase & cache;
        size_t refcount = 0; /// Protected by the cache mutex
    };

    using InsertTokenById = std::unordered_map<Key, std::shared_ptr<InsertToken>, HashFunction>;

    /// This class is responsible for removing used insert tokens from the insert_tokens map.
    /// Among several concurrent threads the first successful one is responsible for removal. But if they all
    /// fail, then the last one is responsible.
    struct InsertTokenHolder
    {
        const Key * key = nullptr;
        std::shared_ptr<InsertToken> token;
        bool cleaned_up = false;

        InsertTokenHolder() = default;

        void acquire(const Key * key_, const std::shared_ptr<InsertToken> & token_, std::lock_guard<std::mutex> & /* cache_lock */)
            TSA_NO_THREAD_SAFETY_ANALYSIS // disabled only because we can't reference the parent-level cache mutex from here
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void cleanup(std::lock_guard<std::mutex> & /* token_lock */, std::lock_guard<std::mutex> & /* cache_lock */)
            TSA_NO_THREAD_SAFETY_ANALYSIS // disabled only because we can't reference the parent-level cache mutex from here
        {
            token->cache.insert_tokens.erase(*key);
            token->cleaned_up = true;
            cleaned_up = true;
        }

        ~InsertTokenHolder()
        {
            if (!token)
                return;

            if (cleaned_up)
                return;

            std::lock_guard token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::lock_guard cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                cleanup(token_lock, cache_lock);
        }
    };

    friend struct InsertTokenHolder;

    InsertTokenById insert_tokens TSA_GUARDED_BY(mutex);

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
};


}
