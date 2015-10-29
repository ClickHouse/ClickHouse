#include <DB/Interpreters/Cluster.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Common/isLocalAddress.h>
#include <DB/Common/SimpleCache.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

namespace DB
{

namespace
{

/// Вес шарда по-умолчанию.
static constexpr int default_weight = 1;

inline bool isLocal(const Cluster::Address & address)
{
	///	Если среди реплик существует такая, что:
	/// - её порт совпадает с портом, который слушает сервер;
	/// - её хост резолвится в набор адресов, один из которых совпадает с одним из адресов сетевых интерфейсов сервера
	/// то нужно всегда ходить на этот шард без межпроцессного взаимодействия
	return isLocalAddress(address.resolved_address);
}

inline std::string addressToDirName(const Cluster::Address & address)
{
	return
		escapeForFileName(address.user) +
		(address.password.empty() ? "" : (':' + escapeForFileName(address.password))) + '@' +
		escapeForFileName(address.resolved_address.host().toString()) + ':' +
		std::to_string(address.resolved_address.port());
}

inline bool beginsWith(const std::string & str1, const char * str2)
{
	if (str2 == nullptr)
		throw Exception("Passed null pointer to function beginsWith", ErrorCodes::LOGICAL_ERROR);
	return 0 == strncmp(str1.data(), str2, strlen(str2));
}

/// Для кэширования DNS запросов.
Poco::Net::SocketAddress resolveSocketAddressImpl1(const String & host, UInt16 port)
{
	return Poco::Net::SocketAddress(host, port);
}

Poco::Net::SocketAddress resolveSocketAddressImpl2(const String & host_and_port)
{
	return Poco::Net::SocketAddress(host_and_port);
}

Poco::Net::SocketAddress resolveSocketAddress(const String & host, UInt16 port)
{
	static SimpleCache<decltype(resolveSocketAddressImpl1), &resolveSocketAddressImpl1> cache;
	return cache(host, port);
}

Poco::Net::SocketAddress resolveSocketAddress(const String & host_and_port)
{
	static SimpleCache<decltype(resolveSocketAddressImpl2), &resolveSocketAddressImpl2> cache;
	return cache(host_and_port);
}

}

/// Реализация класса Cluster::Address

Cluster::Address::Address(const String & config_prefix)
{
	const auto & config = Poco::Util::Application::instance().config();

	host_name = config.getString(config_prefix + ".host");
	port = config.getInt(config_prefix + ".port");
	resolved_address = resolveSocketAddress(host_name, port);
	user = config.getString(config_prefix + ".user", "default");
	password = config.getString(config_prefix + ".password", "");
}


Cluster::Address::Address(const String & host_port_, const String & user_, const String & password_)
	: user(user_), password(password_)
{
	UInt16 default_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);

	/// Похоже на то, что строка host_port_ содержит порт. Если условие срабатывает - не обязательно значит, что порт есть (пример: [::]).
	if ((nullptr != strchr(host_port_.c_str(), ':')) || !default_port)
	{
		resolved_address = resolveSocketAddress(host_port_);
		host_name = host_port_.substr(0, host_port_.find(':'));
		port = resolved_address.port();
	}
	else
	{
		resolved_address = resolveSocketAddress(host_port_, default_port);
		host_name = host_port_;
		port = default_port;
	}
}

/// Реализация класса Clusters

Clusters::Clusters(const Settings & settings, const String & config_name)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();
	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(config_name, config_keys);

	for (const auto & key : config_keys)
		impl.emplace(std::piecewise_construct,
			std::forward_as_tuple(key),
			std::forward_as_tuple(settings, config_name + "." + key));
}

/// Реализация класса Cluster

Cluster::Cluster(const Settings & settings, const String & cluster_name)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();
	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(cluster_name, config_keys);

	const auto & config_prefix = cluster_name + ".";

	UInt32 current_shard_num = 1;

	for (const auto & key : config_keys)
	{
		if (beginsWith(key, "node"))
		{
			/// Шард без реплик.

			const auto & prefix = config_prefix + key;
			const auto weight = config.getInt(prefix + ".weight", default_weight);
			if (weight == 0)
				continue;

			addresses.emplace_back(prefix);
			addresses.back().replica_num = 1;
			const auto & address = addresses.back();

			ShardInfo info;
			info.shard_num = current_shard_num;
			info.weight = weight;

			if (isLocal(address))
				info.local_addresses.push_back(address);
			else
			{
				info.dir_names.push_back(addressToDirName(address));
				info.pool = new ConnectionPool(
					settings.distributed_connections_pool_size,
					address.host_name, address.port, address.resolved_address,
					"", address.user, address.password,
					"server", Protocol::Compression::Enable,
					saturate(settings.connect_timeout, settings.limits.max_execution_time),
					saturate(settings.receive_timeout, settings.limits.max_execution_time),
					saturate(settings.send_timeout, settings.limits.max_execution_time));
			}

			slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());
			shards_info.push_back(info);
		}
		else if (beginsWith(key, "shard"))
		{
			/// Шард с репликами.

			Poco::Util::AbstractConfiguration::Keys replica_keys;
			config.keys(config_prefix + key, replica_keys);

			addresses_with_failover.emplace_back();
			Addresses & replica_addresses = addresses_with_failover.back();
			UInt32 current_replica_num = 1;

			const auto & partial_prefix = config_prefix + key + ".";
			const auto weight = config.getInt(partial_prefix + ".weight", default_weight);
			if (weight == 0)
				continue;

			const auto internal_replication = config.getBool(partial_prefix + ".internal_replication", false);

			/** in case of internal_replication we will be appending names to
			 *  the first element of vector; otherwise we will just .emplace_back
			 */
			std::vector<std::string> dir_names{};

			auto first = true;
			for (const auto & replica_key : replica_keys)
			{
				if (beginsWith(replica_key, "weight") || beginsWith(replica_key, "internal_replication"))
					continue;

				if (beginsWith(replica_key, "replica"))
				{
					replica_addresses.emplace_back(partial_prefix + replica_key);
					replica_addresses.back().replica_num = current_replica_num;
					++current_replica_num;

					if (!isLocal(replica_addresses.back()))
					{
						if (internal_replication)
						{
							auto dir_name = addressToDirName(replica_addresses.back());
							if (first)
								dir_names.emplace_back(std::move(dir_name));
							else
								dir_names.front() += "," + dir_name;
						}
						else
							dir_names.emplace_back(addressToDirName(replica_addresses.back()));

						if (first) first = false;
					}
				}
				else
					throw Exception("Unknown element in config: " + replica_key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
			}

			Addresses shard_local_addresses;

			ConnectionPools replicas;
			replicas.reserve(replica_addresses.size());

			for (const auto & replica : replica_addresses)
			{
				if (isLocal(replica))
					shard_local_addresses.push_back(replica);
				else
				{
					replicas.emplace_back(new ConnectionPool(
						settings.distributed_connections_pool_size,
						replica.host_name, replica.port, replica.resolved_address,
						"", replica.user, replica.password,
						"server", Protocol::Compression::Enable,
						saturate(settings.connect_timeout_with_failover_ms, settings.limits.max_execution_time),
						saturate(settings.receive_timeout, settings.limits.max_execution_time),
						saturate(settings.send_timeout, settings.limits.max_execution_time)));
				}
			}

			ConnectionPoolPtr shard_pool;
			if (!replicas.empty())
				shard_pool = new ConnectionPoolWithFailover(replicas, settings.load_balancing, settings.connections_with_failover_max_tries);

			slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());
			shards_info.push_back({std::move(dir_names), current_shard_num, weight, shard_local_addresses, shard_pool});
		}
		else
			throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

		if (!addresses_with_failover.empty() && !addresses.empty())
			throw Exception("There must be either 'node' or 'shard' elements in config", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

		++current_shard_num;
	}

	initMisc();
}


Cluster::Cluster(const Settings & settings, std::vector<std::vector<String>> names,
				 const String & username, const String & password)
{
	UInt32 current_shard_num = 1;

	for (const auto & shard : names)
	{
		Addresses current;
		for (auto & replica : shard)
			current.emplace_back(replica, username, password);

		addresses_with_failover.emplace_back(current);

		ConnectionPools replicas;
		replicas.reserve(current.size());

		for (const auto & replica : current)
		{
			replicas.emplace_back(new ConnectionPool(
				settings.distributed_connections_pool_size,
				replica.host_name, replica.port, replica.resolved_address,
				"", replica.user, replica.password,
				"server", Protocol::Compression::Enable,
				saturate(settings.connect_timeout_with_failover_ms, settings.limits.max_execution_time),
				saturate(settings.receive_timeout, settings.limits.max_execution_time),
				saturate(settings.send_timeout, settings.limits.max_execution_time)));
		}

		ConnectionPoolPtr shard_pool = new ConnectionPoolWithFailover(replicas, settings.load_balancing, settings.connections_with_failover_max_tries);

		slot_to_shard.insert(std::end(slot_to_shard), default_weight, shards_info.size());
		shards_info.push_back({{}, current_shard_num, default_weight, {}, shard_pool});
		++current_shard_num;
	}

	initMisc();
}


Poco::Timespan Cluster::saturate(const Poco::Timespan & v, const Poco::Timespan & limit)
{
	if (limit.totalMicroseconds() == 0)
		return v;
	else
		return (v > limit) ? limit : v;
}


void Cluster::initMisc()
{
	for (const auto & shard_info : shards_info)
	{
		if (!shard_info.isLocal() && !shard_info.hasRemoteConnections())
			throw Exception("Found shard without any specified connection",
				ErrorCodes::SHARD_HAS_NO_CONNECTIONS);
	}

	for (const auto & shard_info : shards_info)
	{
		if (shard_info.isLocal())
			++local_shard_count;
		else
			++remote_shard_count;
	}

	for (auto & shard_info : shards_info)
	{
		if (!shard_info.isLocal())
		{
			any_remote_shard_info = &shard_info;
			break;
		}
	}
}

}
