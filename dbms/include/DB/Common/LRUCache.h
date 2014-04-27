#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <Poco/ScopedLock.h>
#include <Poco/Mutex.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Exception.h>

namespace DB
{

template <typename T>
struct TrivialWeightFunction
{
	size_t operator()(const T & x) const
	{
		return 1;
	}
};


/** Кеш, вытесняющий долго не использовавшиеся записи. thread-safe.
  * WeightFunction - тип, оператор () которого принимает Mapped и возвращает "вес" (примерный размер) этого значения.
  * Кеш начинает выбрасывать значения, когда их суммарный вес превышает max_size.
  * После вставки значения его вес не должен меняться.
  */
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TMapped>, typename WeightFunction = TrivialWeightFunction<TMapped> >
class LRUCache
{
public:
	typedef TKey Key;
	typedef TMapped Mapped;
	typedef std::shared_ptr<Mapped> MappedPtr;

	LRUCache(size_t max_size_)
		: max_size(std::max(1ul, max_size_)) {}

	MappedPtr get(const Key & key)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		auto it = cells.find(key);
		if (it == cells.end())
		{
			++misses;
			return MappedPtr();
		}

		++hits;
		Cell & cell = it->second;

		/// Переместим ключ в конец очереди. Итератор остается валидным.
		queue.splice(queue.end(), queue, cell.queue_iterator);

		return cell.value;
	}

	void set(const Key & key, MappedPtr mapped)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		auto res = cells.emplace(std::piecewise_construct,
			std::forward_as_tuple(key),
			std::forward_as_tuple());

		Cell & cell = res.first->second;
		bool inserted = res.second;

		if (inserted)
		{
			cell.queue_iterator = queue.insert(queue.end(), key);
		}
		else
		{
			current_size -= cell.size;
			queue.splice(queue.end(), queue, cell.queue_iterator);
		}

		cell.value = mapped;
		cell.size = cell.value ? weight_function(*cell.value) : 0;
		current_size += cell.size;

		removeOverflow();
	}

	void getStats(size_t & out_hits, size_t & out_misses) const
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		out_hits = hits;
		out_misses = misses;
	}

	size_t weight() const
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return current_size;
	}

	size_t count() const
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return cells.size();
	}

	void reset()
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		queue.clear();
		cells.clear();
		current_size = 0;
		hits = 0;
		misses = 0;
		current_weight_lost = 0;
	}

protected:
	size_t current_weight_lost = 0;
	/// Суммарный вес выброшенных из кеша элементов.
	/// Обнуляется каждый раз, когда информация добавляется в Profile events
private:
	typedef std::list<Key> LRUQueue;
	typedef typename LRUQueue::iterator LRUQueueIterator;

	struct Cell
	{
		MappedPtr value;
		size_t size;
		LRUQueueIterator queue_iterator;
	};

	typedef std::unordered_map<Key, Cell, HashFunction> Cells;

	LRUQueue queue;
	Cells cells;

	/// Суммарный вес значений.
	size_t current_size = 0;
	const size_t max_size;

	mutable Poco::FastMutex mutex;
	size_t hits = 0;
	size_t misses = 0;

	WeightFunction weight_function;

	void removeOverflow()
	{
		size_t queue_size = cells.size();
		while (current_size > max_size && queue_size > 1)
		{
			const Key & key = queue.front();
			auto it = cells.find(key);
			current_size -= it->second.size;
			current_weight_lost += it->second.size;
			cells.erase(it);
			queue.pop_front();
			--queue_size;
		}

		if (current_size > (1ull << 63))
		{
			queue.clear();
			cells.clear();
			current_size = 0;
			throw Exception("LRUCache became inconsistent. There must be a bug in it. Clearing it for now.",
				ErrorCodes::LOGICAL_ERROR);
		}
	}
};


}
