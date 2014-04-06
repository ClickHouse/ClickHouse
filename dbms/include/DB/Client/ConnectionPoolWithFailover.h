#pragma once

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <DB/Client/ConnectionPool.h>


namespace DB
{

/** Пул соединений с отказоустойчивостью.
  * Инициализируется несколькими другими IConnectionPool-ами.
  * При получении соединения, пытается создать или выбрать живое соединение из какого-нибудь пула,
  *  перебирая их в некотором порядке, используя не более указанного количества попыток.
  * Предпочитаются пулы с меньшим количеством ошибок;
  *  пулы с одинаковым количеством ошибок пробуются в случайном порядке.
  *
  * Замечание: если один из вложенных пулов заблокируется из-за переполнения, то этот пул тоже заблокируется.
  */
class ConnectionPoolWithFailover : public IConnectionPool
{
public:
	typedef detail::ConnectionPoolEntry Entry;

	ConnectionPoolWithFailover(ConnectionPools & nested_pools_,
		LoadBalancing load_balancing,
		size_t max_tries_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
		time_t decrease_error_period_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD)
	   : nested_pools(nested_pools_.begin(), nested_pools_.end(), decrease_error_period_), max_tries(max_tries_),
	   log(&Logger::get("ConnectionPoolWithFailover")), default_load_balancing(load_balancing)
	{
	}

	/** Выделяет соединение для работы. */
	Entry get(Settings * settings)
	{
		LoadBalancing load_balancing = default_load_balancing;
		if (settings)
			load_balancing = settings->load_balancing;

		/// Обновление случайных чисел, а также счётчиков ошибок.
		nested_pools.update(load_balancing);

		typedef std::vector<PoolWithErrorCount*> PoolPtrs;

		size_t pools_size = nested_pools.size();
		PoolPtrs pool_ptrs(pools_size);
		for (size_t i = 0; i < pools_size; ++i)
			pool_ptrs[i] = &nested_pools[i];

		std::sort(pool_ptrs.begin(), pool_ptrs.end(),
			[=](const PoolPtrs::value_type & lhs, const PoolPtrs::value_type & rhs)
			{
				return PoolWithErrorCount::compare(*lhs, *rhs, load_balancing);
			});

		std::stringstream fail_messages;

		for (size_t try_no = 0; try_no < max_tries; ++try_no)
		{
			for (size_t i = 0; i < pools_size; ++i)
			{
				std::stringstream fail_message;

				try
				{
					Entry res = pool_ptrs[i]->pool->get(settings);
					res->forceConnected();
					return res;
				}
			    catch (const Exception & e)
				{
					if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT)
						throw;

					fail_message << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
				}

				LOG_WARNING(log, "Connection failed at try №"
					<< (try_no + 1) << ", reason: " << fail_message.str());

				fail_messages << fail_message.str() << std::endl;

				__sync_fetch_and_add(&pool_ptrs[i]->random_error_count, 1);
				__sync_fetch_and_add(&pool_ptrs[i]->nearest_hostname_error_count, 1);
			}
		}

		throw Exception("All connection tries failed. Log: \n\n" + fail_messages.str() + "\n",
			ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
	}


private:
	struct PoolWithErrorCount
	{
		ConnectionPoolPtr pool;

		UInt64 random_error_count = 0;
		UInt32 random = 0;
		drand48_data rand_state;

		/// берётся имя локального сервера (Poco::Net::DNS::hostName) и имя хоста из конфига; строки обрезаются до минимальной длины;
		/// затем считается количество отличающихся позиций
		/// Пример example01-01-1 и example01-02-2 отличаются в двух позициях.
		size_t hostname_difference = 0;
		UInt64 nearest_hostname_error_count = 0;

		PoolWithErrorCount(const ConnectionPoolPtr & pool_) : pool(pool_)
		{
			/// Инициализация плохая, но это не важно.
			srand48_r(reinterpret_cast<intptr_t>(this), &rand_state);

			std::string local_hostname = Poco::Net::DNS::hostName();

			ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*pool);
			const std::string & host = connection_pool.getHost();
			hostname_difference = 0;
			for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
				if (local_hostname[i] != host[i])
					++hostname_difference;
		}

		void randomize()
		{
			long int rand_res;
			lrand48_r(&rand_state, &rand_res);
			random = rand_res;
		}

		static bool compare(const PoolWithErrorCount & lhs, const PoolWithErrorCount & rhs, LoadBalancing load_balancing_mode)
		{
			if (load_balancing_mode == LoadBalancing::RANDOM)
			{
				return std::tie(lhs.random_error_count, lhs.random)
					< std::tie(rhs.random_error_count, rhs.random);
			}
			else if (load_balancing_mode == LoadBalancing::NEAREST_HOSTNAME)
			{
				return std::tie(lhs.nearest_hostname_error_count, lhs.hostname_difference)
					< std::tie(rhs.nearest_hostname_error_count, rhs.hostname_difference);
			}
			else
				throw Exception("Unknown load_balancing_mode: " + toString(static_cast<int>(load_balancing_mode)), ErrorCodes::LOGICAL_ERROR);
		}
	};


	class PoolsWithErrorCount : public std::vector<PoolWithErrorCount>
	{
	public:
		PoolsWithErrorCount(DB::ConnectionPools::iterator begin_, DB::ConnectionPools::iterator end_,
							time_t decrease_error_period_)
			: std::vector<PoolWithErrorCount>(begin_, end_),
			decrease_error_period(decrease_error_period_)
		{
		}

		void update(LoadBalancing load_balancing_mode)
		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);

			switch (load_balancing_mode)
			{
				case LoadBalancing::RANDOM:
				{
					for (PoolsWithErrorCount::iterator it = begin(); it != end(); ++it)
						it->randomize();
					/// NOTE Почему бы не делить счётчики ошибок в случае LoadBalancing::RANDOM тоже?
					break;
				}

				case LoadBalancing::NEAREST_HOSTNAME:
				{
					/// Для режима NEAREST_HOSTNAME каждые N секунд уменьшаем количество ошибок в 2 раза
					time_t current_time = time(0);

					if (last_decrease_time)
					{
						time_t delta = current_time - last_decrease_time;

						if (delta < 0)
							return;

						/// Каждые decrease_error_period секунд, делим количество ошибок на два.
						size_t shift_amount = delta / decrease_error_period;

						if (shift_amount > sizeof(UInt64))
						{
							last_decrease_time = current_time;
							for (PoolsWithErrorCount::iterator it = begin(); it != end(); ++it)
								it->nearest_hostname_error_count = 0;
						}
						else if (shift_amount)
						{
							last_decrease_time = current_time;
							for (PoolsWithErrorCount::iterator it = begin(); it != end(); ++it)
								it->nearest_hostname_error_count >>= shift_amount;
						}
					}
					else
						last_decrease_time = current_time;

					break;
				}

				default:
					throw Exception("Unknown load_balancing_mode: " + toString(static_cast<int>(load_balancing_mode)), ErrorCodes::LOGICAL_ERROR);
			}
		}

	private:
		/// Время, когда последний раз уменьшался счётчик ошибок для LoadBalancing::NEAREST_HOSTNAME
		time_t last_decrease_time = 0;
		time_t decrease_error_period;
		Poco::FastMutex mutex;
	};

	PoolsWithErrorCount nested_pools;
	size_t max_tries;
	Logger * log;
	LoadBalancing default_load_balancing;
};


}
