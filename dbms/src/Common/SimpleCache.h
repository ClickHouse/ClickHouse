#pragma once

#include <map>
#include <tuple>
#include <mutex>
#include <ext/function_traits.hpp>


/** The simplest cache for a free function.
  * You can also pass a static class method or lambda without captures.
  * The size is unlimited. Values are stored permanently and never evicted.
  * Mutex is used for synchronization.
  * Suitable only for the simplest cases.
  *
  * Usage
  *
  * SimpleCache<decltype(func), &func> func_cached;
  * std::cerr << func_cached(args...);
  */
template <typename F, F* f>
class SimpleCache
{
private:
    using Key = typename function_traits<F>::arguments_decay;
    using Result = typename function_traits<F>::result;

    std::map<Key, Result> cache;
    std::mutex mutex;

public:
    template <typename... Args>
    Result operator() (Args &&... args)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);

            Key key{std::forward<Args>(args)...};
            auto it = cache.find(key);

            if (cache.end() != it)
                return it->second;
        }

        /// The calculations themselves are not done under mutex.
        Result res = f(std::forward<Args>(args)...);

        {
            std::lock_guard<std::mutex> lock(mutex);

            cache.emplace(std::forward_as_tuple(args...), res);
        }

        return res;
    }
};
