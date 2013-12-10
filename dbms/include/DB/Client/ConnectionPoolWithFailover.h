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

	ConnectionPoolWithFailover(ConnectionPools & nested_pools_, size_t load_balancing_mode_,
							size_t max_tries_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
							size_t decrease_error_period_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD)
	   : nested_pools(nested_pools_.begin(), nested_pools_.end(), load_balancing_mode_, decrease_error_period_), max_tries(max_tries_),
	   log(&Logger::get("ConnectionPoolWithFailover"))
	{
	}

	/** Выделяет соединение для работы. */
	Entry get()
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		
		nested_pools.update();
		std::sort(nested_pools.begin(), nested_pools.end());

		std::stringstream fail_messages;

		for (size_t try_no = 0; try_no < max_tries; ++try_no)
		{
			for (size_t i = 0, size = nested_pools.size(); i < size; ++i)
			{
				std::stringstream fail_message;

				try
				{
					Entry res = nested_pools[i].pool->get();
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

				++nested_pools[i].error_count;
			}
		}

		throw Exception("All connection tries failed. Log: \n\n" + fail_messages.str() + "\n",
			ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
	}

private:
	struct PoolWithErrorCount
	{
		ConnectionPoolPtr pool;
		UInt64 error_count;
		UInt32 random;
		drand48_data rand_state;
		size_t load_balancing_mode;

		PoolWithErrorCount(const ConnectionPoolPtr & pool_)
			: pool(pool_), error_count(0), random(0)
		{
			/// Инициализация плохая, но это не важно.
			srand48_r(reinterpret_cast<ptrdiff_t>(this), &rand_state);

			std::string local_hostname = Poco::Net::DNS::hostName();


			ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*pool);
			const std::string & host = connection_pool.getHost();
			hostname_difference = 0;
			for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
			{
				if (local_hostname[i] != host[i])
					++hostname_difference;
			}
		}
		
		void randomize()
		{
			long int rand_res;
			lrand48_r(&rand_state, &rand_res);
			random = rand_res;
		}

		bool operator< (const PoolWithErrorCount & rhs) const
		{
			if (load_balancing_mode == LoadBalancing::RANDOM)
			{
				return error_count < rhs.error_count
					|| (error_count == rhs.error_count && random < rhs.random);
			}
			else if (load_balancing_mode == LoadBalancing::NEAREST_HOSTNAME)
			{
				return error_count < rhs.error_count
					|| (error_count == rhs.error_count && hostname_difference < rhs.hostname_difference);
			}
			else
				throw Poco::Exception("Unsupported LoadBalancing mode = " + load_balancing_mode);
		}

	private:
		/// берётся имя локального сервера (Poco::Net::DNS::hostName) и имя хоста из конфига; строки обрезаются до минимальной длины;
		/// затем считается количество отличающихся позиций
		/// Пример example01-01-1 и example01-02-2 отличаются в двух позициях.
		size_t hostname_difference;
	};

	class PoolsWithErrorCount : public std::vector<PoolWithErrorCount>
	{
	public:
		PoolsWithErrorCount(DB::ConnectionPools::iterator first, DB::ConnectionPools::iterator last,
							size_t load_balancing_mode_, size_t decrease_error_period_) :
							std::vector<PoolWithErrorCount>(first, last), load_balancing_mode(load_balancing_mode_), last_get_time(0),
							decrease_error_period(decrease_error_period_)
		{
			for (PoolsWithErrorCount::iterator it = begin(); it != end(); ++it)
			{
				it->load_balancing_mode = load_balancing_mode;
			}
		}

		void update()
		{
			if (load_balancing_mode == LoadBalancing::RANDOM)
			{
				for (PoolsWithErrorCount::iterator it = begin(); it != end(); ++it)
					it->randomize();
			}
			/// В режиме NEAREST_HOSTNAME каждые N секунд уменьшаем количество ошибок в 2 раза
			else if (last_get_time && load_balancing_mode == LoadBalancing::NEAREST_HOSTNAME)
			{
				time_t delta = time(0) - last_get_time;
				for (PoolsWithErrorCount::iterator it = begin(); it != end(); ++it)
				{
					it->error_count	= it->error_count >> (delta/decrease_error_period);
				}
			}
			last_get_time = time(0);
		}

	private:
		size_t load_balancing_mode;

		/// время, когда последний раз вызывался update
		time_t last_get_time;
		time_t decrease_error_period;
	};

	Poco::FastMutex mutex;

	PoolsWithErrorCount nested_pools;

	size_t max_tries;

	Logger * log;
};


}
