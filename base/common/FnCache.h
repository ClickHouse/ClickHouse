#pragma once

#include <map>
#include <mutex>
#include "FnTraits.h"

/**
 * The simplest cache for a functor that decays to a pointer-to-function.
 * The size is unlimited. Values are stored permanently and never evicted.
 * Suitable only for the simplest cases.
 *
 * Invocation/update is O(log(saved cache values)).
 *
 * @example FnCache<&my_func> cached; cached(arg);
  */
template <auto * Func>
struct FnCache
{
private:
    using Traits = FnTraits<decltype(Func)>;
    using Key = typename Traits::DecayedArgs;
    using Result = typename Traits::Ret;

    std::map<Key, Result> cache; // Can't use hashmap as tuples are unhashable by default
    mutable std::mutex mutex;

public:
    template <typename... Args>
    Result operator()(Args && ...args)
    {
        Key key{std::forward<Args>(args)...};

        {
            std::lock_guard lock(mutex);

            if (auto it = cache.find(key); it != cache.end())
                return it->second;
        }

        /// The calculations themselves are not done under mutex.
        Result res = std::apply(Func, key);

        {
            std::lock_guard lock(mutex);
            cache.emplace(std::move(key), res);
        }

        return res;
    }

    template <typename ...Args>
    void update(Args && ...args)
    {
        Key key{std::forward<Args>(args)...};
        Result res = std::apply(Func, key);

        {
            std::lock_guard lock(mutex);
            cache.emplace(std::move(key), std::move(res));
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
