#pragma once

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <DB/Common/PoolWithFailoverBase.h>

#include <DB/Common/getFQDNOrHostName.h>
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
class ConnectionPoolWithFailover : public PoolWithFailoverBase<IConnectionPool>, public IConnectionPool
{
public:
	typedef IConnectionPool::Entry Entry;
	typedef PoolWithFailoverBase<IConnectionPool> Base;

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
	/** Выделяет соединение для работы. */
	Entry doGet(const Settings * settings) override
	{
		applyLoadBalancing(settings);
		return Base::get(settings);
	}

	/** Выделяет до указанного количества соединений для работы.
	  * Соединения предоставляют доступ к разным репликам одного шарда.
	  */
	std::vector<Entry> doGetMany(const Settings * settings, bool get_all) override
	{
		applyLoadBalancing(settings);
		return Base::getMany(settings, get_all);
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
	std::vector<size_t> hostname_differences; /// Расстояния от имени этого хоста до имен хостов пулов.
	LoadBalancing default_load_balancing;
};


}
