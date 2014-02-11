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
		: max_size(std::max(1ul, max_size_)), current_size(0), hits(0), misses(0) {}

	MappedPtr get(const Key & key)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		CellsIterator it = cells.find(key);
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

		std::pair<CellsIterator, bool> it =
			cells.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());
		Cell & cell = it.first->second;
		bool inserted = it.second;

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

	void getStats(size_t & out_hits, size_t & out_misses) const volatile
	{
		/// Синхронизация не нужна.
		out_hits = hits;
		out_misses = misses;
	}

	size_t weight() const
	{
		return current_size;
	}

	size_t count() const
	{
		return queue.size();
	}
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
	typedef typename Cells::iterator CellsIterator;

	LRUQueue queue;
	Cells cells;
	size_t max_size;
	size_t current_size;

	Poco::FastMutex mutex;
	size_t hits;
	size_t misses;

	WeightFunction weight_function;

	void removeOverflow()
	{
		while (current_size > max_size && queue.size() > 1)
		{
			const Key & key = queue.front();
			CellsIterator it = cells.find(key);
			current_size -= it->second.size;
			cells.erase(it);
			queue.pop_front();
		}

		if (queue.size() != cells.size() || current_size > (1ull << 63))
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
