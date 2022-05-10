#pragma once

#include <Common/ICachePolicy.h>
#include <Common/LRUCachePolicy.h>
#include <Common/SLRUCachePolicy.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <base/logger_useful.h>


namespace DB
{

/// Thread-safe cache that evicts entries using special cache policy
/// (default policy evicts entries which are not used for a long time).
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialWeightFunction<TMapped>>
class CacheBase
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    CacheBase(size_t max_size, size_t max_elements_size = 0) : CacheBase("LRU", max_size, max_elements_size) {}

    /// TODO: Rewrite "Args... args" to custom struct with fields for all cache policies.
    template <class... Args>
    CacheBase(String cache_policy_name, Args... args)
    {
        auto onWeightLossFunction = [&](size_t weight_loss) { onRemoveOverflowWeightLoss(weight_loss); };

        if (cache_policy_name == "")
        {
            cache_policy_name = default_cache_policy_name;
        }

        policy_name = cache_policy_name;

        LOG_DEBUG(&Poco::Logger::get("CacheBase"), "Cache policy name \"{}\"", cache_policy_name);
        if (cache_policy_name == "LRU")
        {
            using LRUPolicy = LRUCachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
            cache_policy = std::make_unique<LRUPolicy>(onWeightLossFunction, args...);
        }
        else if (cache_policy_name == "SLRU")
        {
            using SLRUPolicy = SLRUCachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
            cache_policy = std::make_unique<SLRUPolicy>(onWeightLossFunction, args...);
        }
        else
        {
            LOG_ERROR(&Poco::Logger::get("CacheBase"), "Undeclared cache policy name \"{}\"", cache_policy_name);
            abort();
        }
    }

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
        /// (The token may be absent because of a concurrent reset() call).
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

    void reset()
    {
        std::lock_guard lock(mutex);
        insert_tokens.clear();
        hits = 0;
        misses = 0;
        cache_policy->reset();
    }

    void remove(const Key & key)
    {
        std::lock_guard lock(mutex);
        cache_policy->remove(key);
    }

    size_t weight() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->weight();
    }

    size_t count() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->count();
    }

    size_t maxSize() const
    {
        std::lock_guard lock(mutex);
        return cache_policy->maxSize();
    }

    virtual ~CacheBase() = default;

protected:
    mutable std::mutex mutex;

private:
    using CachePolicy = ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>;

    std::unique_ptr<CachePolicy> cache_policy;
    String policy_name; // DELETE THIS FIELD (only for debug)

    inline static const String default_cache_policy_name = "LRU";

    std::atomic<size_t> hits{0};
    std::atomic<size_t> misses{0};

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(CacheBase & cache_) : cache(cache_) {}

        std::mutex mutex;
        bool cleaned_up = false; /// Protected by the token mutex
        MappedPtr value; /// Protected by the token mutex

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

        void acquire(const Key * key_, const std::shared_ptr<InsertToken> & token_, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void cleanup([[maybe_unused]] std::lock_guard<std::mutex> & token_lock, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
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

    InsertTokenById insert_tokens;

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
};


}
