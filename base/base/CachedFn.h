#pragma once

#include <map>
#include <tuple>
#include <mutex>
#include "FnTraits.h"

/**
 * Caching proxy for a functor that decays to a pointer-to-function.
 * Saves pairs (func args, func result on args).
 * Cache size is unlimited. Cache items are evicted only on manual drop.
 * Invocation/update is O(log(saved cache values)).
 *
 * See Common/tests/cached_fn.cpp for examples.
  */
template <auto * Func>
struct CachedFn
{
private:
    using Traits = FnTraits<decltype(Func)>;
    using DecayedArgs = TypeListMap<std::decay_t, typename Traits::Args>;
    using Key = TypeListChangeRoot<std::tuple, DecayedArgs>;
    using Result = typename Traits::Ret;

    std::map<Key, Result> cache; // Can't use hashmap as tuples are unhashable by default
    mutable std::mutex mutex;

public:
    template <class ...Args>
    Result operator()(Args && ...args)
    {
        Key key = std::make_tuple(std::forward<Args>(args)...);

        {
            std::lock_guard lock(mutex);

            if (auto it = cache.find(key); it != cache.end())
                return it->second;
        }

        Result res = std::apply(Func, key);

        {
            std::lock_guard lock(mutex);
            cache.emplace(std::move(key), res);
        }

        return res;
    }

    template <class ...Args>
    void update(Args && ...args)
    {
        Key key = std::make_tuple(std::forward<Args>(args)...);
        Result res = std::apply(Func, key);

        {
            std::lock_guard lock(mutex);
            // TODO Can't use emplace(std::move(key), ..), causes test_host_ip_change errors.
            cache[key] = std::move(res);
        }
    }

    size_t size() const
    {
        std::lock_guard lock(mutex);
        return cache.size();
    }

    void drop()
    {
        std::lock_guard lock(mutex);
        cache.clear();
    }
};
