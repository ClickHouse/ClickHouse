#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <chrono>
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

/** Кеш, вытесняющий долго не использовавшиеся и устаревшие записи. thread-safe.
  * WeightFunction - тип, оператор () которого принимает Mapped и возвращает "вес" (примерный размер) этого значения.
  * Кеш начинает выбрасывать значения, когда их суммарный вес превышает max_size и срок годности этих значений истёк.
  * После вставки значения его вес не должен меняться.
  */
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TMapped>, typename WeightFunction = TrivialWeightFunction<TMapped> >
class LRUCache
{
public:
	using Key = TKey;
	using Mapped = TMapped;
	using MappedPtr = std::shared_ptr<Mapped>;
	using Delay = std::chrono::seconds;

private:
	using Clock = std::chrono::steady_clock;
	using Timestamp = Clock::time_point;

public:
	LRUCache(size_t max_size_, const Delay & expiration_delay_ = Delay::zero())
		: max_size(std::max(1ul, max_size_)), expiration_delay(expiration_delay_) {}

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
		updateCellTimestamp(cell);

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
		updateCellTimestamp(cell);

		removeOverflow(cell.timestamp);
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
	using LRUQueue = std::list<Key>;
	using LRUQueueIterator = typename LRUQueue::iterator;

	struct Cell
	{
	public:
		bool expired(const Timestamp & last_timestamp, const Delay & expiration_delay) const
		{
			return (expiration_delay == Delay::zero()) ||
				((last_timestamp > timestamp) && ((last_timestamp - timestamp) > expiration_delay));
		}

	public:
		MappedPtr value;
		size_t size;
		LRUQueueIterator queue_iterator;
		Timestamp timestamp;
	};

	using Cells = std::unordered_map<Key, Cell, HashFunction>;

	LRUQueue queue;
	Cells cells;

	/// Суммарный вес значений.
	size_t current_size = 0;
	const size_t max_size;
	const Delay expiration_delay;

	mutable Poco::FastMutex mutex;
	size_t hits = 0;
	size_t misses = 0;

	WeightFunction weight_function;

	void updateCellTimestamp(Cell & cell)
	{
		if (expiration_delay != Delay::zero())
			cell.timestamp = Clock::now();
	}

	void removeOverflow(const Timestamp & last_timestamp)
	{
		size_t queue_size = cells.size();
		while ((current_size > max_size) && (queue_size > 1))
		{
			const Key & key = queue.front();

			auto it = cells.find(key);
			if (it == cells.end())
				throw Exception("LRUCache became inconsistent. There must be a bug in it. Clearing it for now.",
					ErrorCodes::LOGICAL_ERROR);

			const auto & cell = it->second;
			if (!cell.expired(last_timestamp, expiration_delay))
				break;

			current_size -= cell.size;
			current_weight_lost += cell.size;

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
