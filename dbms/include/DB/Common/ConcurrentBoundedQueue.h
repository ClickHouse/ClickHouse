#pragma once

#include <queue>

#include <Poco/Mutex.h>
#include <Poco/Semaphore.h>

#include <DB/Core/Types.h>


/** Очень простая thread-safe очередь ограниченной длины.
  * Если пытаться вынуть элемент из пустой очереди, то поток блокируется, пока очередь не станет непустой.
  * Если пытаться вставить элемент в переполненную очередь, то поток блокируется, пока в очереди не появится элемент.
  */
template <typename T>
class ConcurrentBoundedQueue
{
private:
	size_t max_fill;
	std::queue<T> queue;
	Poco::Mutex mutex;
	Poco::Semaphore fill_count;
	Poco::Semaphore empty_count;

public:
	ConcurrentBoundedQueue(size_t max_fill)
		: fill_count(0, max_fill), empty_count(max_fill, max_fill) {}

	void push(const T & x)
	{
		empty_count.wait();
		{
			Poco::ScopedLock<Poco::Mutex> lock(mutex);
			queue.push(x);
		}
		fill_count.set();
	}

	void pop(T & x)
	{
		fill_count.wait();
		{
			Poco::ScopedLock<Poco::Mutex> lock(mutex);
			x = queue.front();
			queue.pop();
		}
		empty_count.set();
	}

	bool tryPush(const T & x, DB::UInt64 milliseconds = 0)
	{
		if (empty_count.tryWait(milliseconds))
		{
			{
				Poco::ScopedLock<Poco::Mutex> lock(mutex);
				queue.push(x);
			}
			fill_count.set();
			return true;
		}
		return false;
	}

	bool tryPop(T & x, DB::UInt64 milliseconds = 0)
	{
		if (fill_count.tryWait(milliseconds))
		{
			{
				Poco::ScopedLock<Poco::Mutex> lock(mutex);
				x = queue.front();
				queue.pop();
			}
			empty_count.set();
			return true;
		}
		return false;
	}

	size_t size()
	{
		Poco::ScopedLock<Poco::Mutex> lock(mutex);
		return queue.size();
	}

	void clear()
	{
		while (fill_count.tryWait(0))
		{
			{
				Poco::ScopedLock<Poco::Mutex> lock(mutex);
				queue.pop();
			}
			empty_count.set();
		}
	}
};
