#pragma once

#include <time.h>
#include <cstdlib>
#include <DB/Common/PoolBase.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/NetException.h>
#include <DB/Common/Exception.h>
#include <DB/Common/randomSeed.h>
#include <DB/Interpreters/Settings.h>

namespace DB
{
namespace ErrorCodes
{
	extern const int ALL_CONNECTION_TRIES_FAILED;
	extern const int LOGICAL_ERROR;
}
}

namespace ProfileEvents
{
	extern const Event DistributedConnectionFailTry;
	extern const Event DistributedConnectionFailAtAll;
}


namespace
{
	/** Класс, который употребляется для того, чтобы оптимизировать выделение
	  * нескольких ресурсов в PoolWithFailoverBase. Проверки границ не проводятся,
	  * потому что мы предполагаем, что PoolWithFailoverBase делает все нужные
	  * проверки.
	  */
	class ResourceTracker
	{
	public:
		ResourceTracker(size_t s)
		: handles(s), unallocated_size(s)
		{
			size_t i = 0;
			for (auto & index : handles)
			{
				index = i;
				++i;
			}
		}

		size_t getHandle(size_t i) const
		{
			return handles[i];
		}

		size_t getUnallocatedSize() const
		{
			return unallocated_size;
		}

		void markAsAllocated(size_t i)
		{
			std::swap(handles[i], handles[unallocated_size - 1]);
			--unallocated_size;
		}

	private:
		std::vector<size_t> handles;
		size_t unallocated_size;
	};
}

/** Класс, от которого можно унаследоваться и получить пул с отказоустойчивостью. Используется для пулов соединений с реплицированной БД.
  * Инициализируется несколькими другими PoolBase-ами.
  * При получении соединения, пытается создать или выбрать живое соединение из какого-нибудь пула,
  *  перебирая их в некотором порядке, используя не более указанного количества попыток.
  * Пулы перебираются в порядке лексикографического возрастания тройки (приоритет, число ошибок, случайное число).
  *
  * Замечание: если один из вложенных пулов заблокируется из-за переполнения, то этот пул тоже заблокируется.
  *
  * Наследник должен предоставить метод, достающий соединение из вложенного пула.
  * Еще наследник можнет назначать приоритеты вложенным пулам.
  */

template <typename TNestedPool>
class PoolWithFailoverBase : private boost::noncopyable
{
public:
	using NestedPool = TNestedPool;
	using NestedPoolPtr = std::shared_ptr<NestedPool>;
	using Entry = typename NestedPool::Entry;
	using NestedPools = std::vector<NestedPoolPtr>;

	virtual ~PoolWithFailoverBase() {}

	PoolWithFailoverBase(NestedPools & nested_pools_,
		size_t max_tries_,
		time_t decrease_error_period_,
		Logger * log_)
	   : nested_pools(nested_pools_.begin(), nested_pools_.end(), decrease_error_period_), max_tries(max_tries_),
	   log(log_)
	{
	}

	/** Выделяет соединение для работы. */
	Entry get(const DB::Settings * settings)
	{
		Entry entry;
		std::stringstream fail_messages;

		bool skip_unavailable = settings ? UInt64(settings->skip_unavailable_shards) : false;

		if (getResource(entry, fail_messages, nullptr, settings))
			return entry;
		else if (!skip_unavailable)
			throw DB::NetException("All connection tries failed. Log: \n\n" + fail_messages.str() + "\n", DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
		else
			return {};
	}

	void reportError(const Entry & entry)
	{
		for (auto & pool : nested_pools)
		{
			if (pool.pool->contains(entry))
			{
				++pool.state.error_count;
				return;
			}
		}
		throw DB::Exception("Can't find pool to report error.");
	}

	/** Выделяет до указанного количества соединений для работы
	  * Соединения предоставляют доступ к разным репликам одного шарда.
	  */
	std::vector<Entry> getMany(const DB::Settings * settings, PoolMode pool_mode)
	{
		ResourceTracker resource_tracker{nested_pools.size()};

		UInt64 max_connections;
		if (pool_mode == PoolMode::GET_ALL)
			max_connections = nested_pools.size();
		else if (pool_mode == PoolMode::GET_ONE)
			max_connections = 1;
		else if (pool_mode == PoolMode::GET_MANY)
			max_connections = settings ? UInt64(settings->max_parallel_replicas) : 1;
		else
			throw DB::Exception("Unknown pool allocation mode", DB::ErrorCodes::LOGICAL_ERROR);

		bool skip_unavailable = settings ? UInt64(settings->skip_unavailable_shards) : false;

		std::vector<Entry> connections;
		connections.reserve(max_connections);

		for (UInt64 i = 0; i < max_connections; ++i)
		{
			Entry entry;
			std::stringstream fail_messages;

			if (getResource(entry, fail_messages, &resource_tracker, settings))
				connections.push_back(entry);
			else if ((pool_mode == PoolMode::GET_ALL) || ((i == 0) && !skip_unavailable))
				throw DB::NetException("All connection tries failed. Log: \n\n" + fail_messages.str() + "\n", DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
			else
				break;
		}

		return connections;
	}

protected:
	struct PoolWithErrorCount
	{
	public:
		PoolWithErrorCount(const NestedPoolPtr & pool_) : pool(pool_) {}

		void randomize()
		{
			state.random = rng();
		}

	public:
		struct State
		{
			static bool compare(const State & lhs, const State & rhs)
			{
				return std::forward_as_tuple(lhs.priority, lhs.error_count.load(std::memory_order_relaxed), lhs.random)
					< std::forward_as_tuple(rhs.priority, rhs.error_count.load(std::memory_order_relaxed), rhs.random);
			}

			Int64 priority {0};
			std::atomic<UInt64> error_count {0};
			UInt32 random {0};

			State() {}

			State(const State & other)
				: priority(other.priority),
				error_count(other.error_count.load(std::memory_order_relaxed)),
				random(other.random) {}
		};

	public:
		NestedPoolPtr pool;
		State state;
		std::minstd_rand rng = std::minstd_rand(randomSeed());
	};

	using States = std::vector<typename PoolWithErrorCount::State>;

	class PoolsWithErrorCount : public std::vector<PoolWithErrorCount>
	{
	public:
		PoolsWithErrorCount(typename NestedPools::iterator begin_, typename NestedPools::iterator end_,
							time_t decrease_error_period_)
			: std::vector<PoolWithErrorCount>(begin_, end_),
			decrease_error_period(decrease_error_period_)
		{
		}

		/// Эта функция возвращает собственную копию состояния каждого пула, чтобы не возникло
		/// состояния гонки при выделении соединений.
		States update()
		{
			States states;
			states.reserve(this->size());

			{
				std::lock_guard<std::mutex> lock(mutex);

				for (auto & pool : *this)
					pool.randomize();

				/// Каждые N секунд уменьшаем количество ошибок в 2 раза
				time_t current_time = time(0);

				if (last_decrease_time)
				{
					time_t delta = current_time - last_decrease_time;

					if (delta >= 0)
					{
						/// Каждые decrease_error_period секунд, делим количество ошибок на два.
						size_t shift_amount = delta / decrease_error_period;
						/// обновляем время, не чаще раз в период
						/// в противном случае при частых вызовах счетчик никогда не будет уменьшаться
						if (shift_amount)
							last_decrease_time = current_time;

						if (shift_amount >= sizeof(UInt64))
						{
							for (auto & pool : *this)
								pool.state.error_count = 0;
						}
						else if (shift_amount)
						{
							for (auto & pool : *this)
								pool.state.error_count.store(
									pool.state.error_count.load(std::memory_order_relaxed) >> shift_amount,
									std::memory_order_relaxed);
						}
					}
				}
				else
					last_decrease_time = current_time;

				for (auto & pool : *this)
					states.push_back(pool.state);
			}

			return states;
		}

	private:
		/// Время, когда последний раз уменьшался счётчик ошибок.
		time_t last_decrease_time = 0;
		time_t decrease_error_period;
		std::mutex mutex;
	};

	PoolsWithErrorCount nested_pools;
	size_t max_tries;
	Logger * log;

	virtual bool tryGet(NestedPoolPtr pool, const DB::Settings * settings, Entry & out_entry, std::stringstream & fail_message) = 0;


private:
	/** Выделяет соединение из одной реплики для работы. */
	bool getResource(Entry & entry, std::stringstream & fail_messages, ResourceTracker * resource_tracker, const DB::Settings * settings)
	{
		/// Обновление случайных чисел, а также счётчиков ошибок.
		States states = nested_pools.update();

		struct IndexedPoolWithErrorCount
		{
			PoolWithErrorCount * pool;
			typename PoolWithErrorCount::State * state;
			size_t index;
		};

		using PoolPtrs = std::vector<IndexedPoolWithErrorCount>;

		size_t pools_size = resource_tracker ? resource_tracker->getUnallocatedSize() : nested_pools.size();
		PoolPtrs pool_ptrs(pools_size);

		for (size_t i = 0; i < pools_size; ++i)
		{
			auto & record = pool_ptrs[i];
			size_t pool_index = resource_tracker ? resource_tracker->getHandle(i) : i;
			record.pool = &nested_pools[pool_index];
			record.state = &states[pool_index];
			record.index = i;
		}

		std::sort(pool_ptrs.begin(), pool_ptrs.end(),
			[](const IndexedPoolWithErrorCount & lhs, const IndexedPoolWithErrorCount & rhs)
			{
				return PoolWithErrorCount::State::compare(*(lhs.state), *(rhs.state));
			});

		for (size_t try_no = 0; try_no < max_tries; ++try_no)
		{
			for (size_t i = 0; i < pools_size; ++i)
			{
				std::stringstream fail_message;

				if (tryGet(pool_ptrs[i].pool->pool, settings, entry, fail_message))
				{
					if (resource_tracker)
						resource_tracker->markAsAllocated(pool_ptrs[i].index);
					return true;
				}

				ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

				LOG_WARNING(log, "Connection failed at try №"
					<< (try_no + 1) << ", reason: " << fail_message.str());

				fail_messages << fail_message.str() << std::endl;

				++pool_ptrs[i].pool->state.error_count;
			}
		}

		ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
		return false;
	}
};
