#pragma once

#include <Poco/Net/NetException.h>

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


	ConnectionPoolWithFailover(ConnectionPools & nested_pools_, size_t max_tries_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES)
	   : nested_pools(nested_pools_.begin(), nested_pools_.end()), max_tries(max_tries_), error_counts(nested_pools.size()),
	   log(&Logger::get("ConnectionPoolWithFailover"))
	{
	}


	/** Выделяет соединение для работы. */
	Entry get()
	{
		for (size_t i = 0, size = nested_pools.size(); i < size; ++i)
			nested_pools[i].randomize();

		std::sort(nested_pools.begin(), nested_pools.end());

		std::stringstream fail_messages;

		for (size_t try_no = 0; try_no < max_tries; ++try_no)
		{
			for (size_t i = 0, size = nested_pools.size(); i < size; ++i)
			{
				try
				{
					Entry res = nested_pools[i].pool->get();
					res->forceConnected();
					return res;
				}
			    catch (Poco::Net::NetException & e)
				{
					std::stringstream fail_message;
					
					fail_message << "Poco::Net::NetException. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
						<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
					LOG_WARNING(log, "Connection failed at try №" << try_no << ", reason: " << fail_message);
					
					fail_messages << fail_message.rdbuf() << std::endl;
				}
				catch (Poco::TimeoutException & e)
				{
					std::stringstream fail_message;
					
					fail_message << "Poco::TimeoutException. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
						<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();

					LOG_WARNING(log, "Connection failed at try №" << try_no << ", reason: " << fail_message);

					fail_messages << fail_message.rdbuf() << std::endl;
				}
			}
		}

		throw DB::Exception("All connection tries failed. Log: \n\n" << fail_messages.rdbuf() << "\n",
			ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
	}

private:
	struct PoolWithErrorCount
	{
		ConnectionPoolPtr pool;
		UInt64 error_count;
		UInt32 random;

		PoolWithErrorCount(ConnectionPoolPtr & pool_) : pool(pool_), error_count(0), random(0) {}
		void randomize() { random = rand(); }

		bool operator< (const PoolWithErrorCount & rhs) const
		{
			return error_count < rhs.error_count
				|| (error_count == rhs.error_count && random < rhs.random);
		}
	};

	typedef std::vector<PoolWithErrorCount> PoolsWithErrorCount;
	PoolsWithErrorCount nested_pools;
	
	size_t max_tries;

	typedef std::vector<UInt64> ErrorCounts;
	ErrorCounts error_counts;

	Logger * log;
};


typedef SharedPtr<ConnectionPool> ConnectionPoolPtr;
typedef std::vector<ConnectionPoolPtr> ConnectionPools;

}
