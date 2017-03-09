#pragma once

#include <random>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <DB/Common/PoolWithFailoverBase.h>

#include <DB/Common/getFQDNOrHostName.h>
#include <DB/Client/ConnectionPool.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NETWORK_ERROR;
	extern const int SOCKET_TIMEOUT;
	extern const int LOGICAL_ERROR;
}


/** Connection pool with fault tolerance.
  * Initialized by several other IConnectionPools.
  * When a connection is received, it tries to create or select a live connection from a pool,
  *  fetch them in some order, using no more than the specified number of attempts.
  * Pools with fewer errors are preferred;
  *  pools with the same number of errors are tried in random order.
  *
  * Note: if one of the nested pools is blocked due to overflow, then this pool will also be blocked.
  */
class ConnectionPoolWithFailover : public PoolWithFailoverBase<IConnectionPool>, public IConnectionPool
{
public:
	using Entry = IConnectionPool::Entry;
	using Base = PoolWithFailoverBase<IConnectionPool>;

	ConnectionPoolWithFailover(ConnectionPools & nested_pools_,
		LoadBalancing load_balancing,
		size_t max_tries_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
		time_t decrease_error_period_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD)
	   : Base(nested_pools_, max_tries_, decrease_error_period_,
			&Logger::get("ConnectionPoolWithFailover")), default_load_balancing(load_balancing)
	{
		const std::string & local_hostname = getFQDNOrHostName();

		hostname_differences.resize(nested_pools.size());
		for (size_t i = 0; i < nested_pools.size(); ++i)
		{
			ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i].pool);
			const std::string & host = connection_pool.getHost();

			size_t hostname_difference = 0;
			for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
				if (local_hostname[i] != host[i])
					++hostname_difference;

			hostname_differences[i] = hostname_difference;
		}
	}

protected:
	bool tryGet(ConnectionPoolPtr pool, const Settings * settings, Entry & out_entry, std::stringstream & fail_message) override
	{
		try
		{
			out_entry = pool->get(settings);
			out_entry->forceConnected();
			return true;
		}
		catch (const Exception & e)
		{
			if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT)
				throw;

			fail_message << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
			return false;
		}
	}

private:
	/** Allocates connection to work. */
	Entry doGet(const Settings * settings) override
	{
		applyLoadBalancing(settings);
		return Base::get(settings);
	}

	/** Allocates up to the specified number of connections to work.
	  * Connections provide access to different replicas of one shard.
	  */
	std::vector<Entry> doGetMany(const Settings * settings, PoolMode pool_mode) override
	{
		applyLoadBalancing(settings);
		return Base::getMany(settings, pool_mode);
	}

	void applyLoadBalancing(const Settings * settings)
	{
		LoadBalancing load_balancing = default_load_balancing;
		if (settings)
			load_balancing = settings->load_balancing;

		for (size_t i = 0; i < nested_pools.size(); ++i)
		{
			if (load_balancing == LoadBalancing::NEAREST_HOSTNAME)
				nested_pools[i].state.priority = hostname_differences[i];
			else if (load_balancing == LoadBalancing::RANDOM)
				nested_pools[i].state.priority = 0;
			else if (load_balancing == LoadBalancing::IN_ORDER)
				nested_pools[i].state.priority = i;
			else
				throw Exception("Unknown load_balancing_mode: " + toString(static_cast<int>(load_balancing)), ErrorCodes::LOGICAL_ERROR);
		}
	}

private:
	std::vector<size_t> hostname_differences; /// Distances from name of this host to the names of hosts of pools.
	LoadBalancing default_load_balancing;
};


}
