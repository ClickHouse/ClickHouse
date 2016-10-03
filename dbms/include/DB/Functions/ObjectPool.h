#pragma once
#include <map>
#include <memory>
#include <stack>
#include <mutex>


namespace DB
{


/** Pool for objects that cannot be used from different threads simultaneously.
  * Allows to create an object for each thread.
  * Pool has unbounded size and objects are not destroyed before destruction of pool.
  *
  * Use it in cases when thread local storage is not appropriate
  *  (when maximum number of simultaneously used objects is less
  *   than number of running/sleeping threads, that has ever used object,
  *   and creation/destruction of objects is expensive).
  */
template <typename T, typename Key>
class ObjectPool
{
private:
	struct Holder;
	struct Deleter;

public:
	using Pointer = std::unique_ptr<T, Deleter>;

private:
	///	Holds all objects for same key.
	struct Holder
	{
		std::mutex mutex;
		std::stack<std::unique_ptr<T>> stack;

		/**	Extracts and returns a pointer from the collection if it's not empty,
		  *	 creates a new one by calling provided f() otherwise.
		  */
		template <typename Factory>
		Pointer get(Factory && f)
		{
			std::unique_lock<std::mutex> lock(mutex);

			if (stack.empty())
			{
				lock.unlock();
				return { f(), this };
			}

			auto object = stack.top().release();
			stack.pop();

			return { object, this };
		}
	};

	/**	Specialized deleter for std::unique_ptr.
	  *	Returns underlying pointer back to holder thus reclaiming its ownership.
	  */
	struct Deleter
	{
		Holder * holder;

		Deleter(Holder * holder = nullptr) : holder{holder} {}

		void operator()(T * owning_ptr) const
		{
			std::lock_guard<std::mutex> lock{holder->mutex};
			holder->stack.emplace(owning_ptr);
		}
	};

	/// Key -> objects
	using Container = std::map<Key, std::unique_ptr<Holder>>;

	Container container;
	std::mutex mutex;

public:

	/// f is a function that takes zero arguments (usually captures key from outside scope)
	///  and returns plain pointer to new created object.
	template <typename Factory>
	Pointer get(const Key & key, Factory && f)
	{
		typename Container::iterator it;

		{
			std::unique_lock<std::mutex> lock(mutex);

			it = container.find(key);
			if (container.end() == it)
				it = container.emplace(key, std::make_unique<Holder>()).first;
		}

		return it->second->get(std::forward<Factory>(f));
	}
};


}
