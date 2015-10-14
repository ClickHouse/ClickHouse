#pragma once

#include <map>
#include <tuple>
#include <mutex>
#include <ext/function_traits.hpp>


/** Простейший кэш для свободной функции.
  * Можете также передать статический метод класса или лямбду без захвата.
  * Размер неограничен. Значения не устаревают.
  * Для синхронизации используется mutex.
  * Подходит только для простейших случаев.
  *
  * Использование:
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

		/// Сами вычисления делаются не под mutex-ом.
		Result res = f(std::forward<Args>(args)...);

		{
			std::lock_guard<std::mutex> lock(mutex);

			cache.emplace(std::forward_as_tuple(args...), res);
		}

		return res;
	}
};
