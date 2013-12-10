#include <DB/Interpreters/Cluster.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/NetworkInterface.h>
#include <boost/bind.hpp>

namespace DB
{

Cluster::Address::Address(const std::string & config_prefix)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();
	host_port = Poco::Net::SocketAddress(config.getString(config_prefix + ".host"),
										config.getInt(config_prefix + ".port"));

	user = config.getString(config_prefix + ".user", "default");
	password = config.getString(config_prefix + ".password", "");
}

Clusters::Clusters(const Settings & settings, const DataTypeFactory & data_type_factory, const std::string & config_name)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();
	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(config_name, config_keys);

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		insert(value_type(*it, Cluster(settings, data_type_factory, config_name + "." + *it)));
	}
}

Cluster::Address::Address(const Poco::Net::SocketAddress & host_port_, const String & user_, const String & password_)
	: host_port(host_port_), user(user_), password(password_) {}

Cluster::Cluster(const Settings & settings, const DataTypeFactory & data_type_factory, const std::string & cluster_name):
	local_nodes_num(0)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();
	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(cluster_name, config_keys);

	String config_prefix = cluster_name + ".";

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		if (0 == strncmp(it->c_str(), "node", strlen("node")))
		{
			addresses.push_back(Address(config_prefix + *it));
		}
		else if (0 == strncmp(it->c_str(), "shard", strlen("shard")))
		{
			Poco::Util::AbstractConfiguration::Keys replica_keys;
			config.keys(config_prefix + *it, replica_keys);

			addresses_with_failover.push_back(Addresses());
			Addresses & replica_addresses = addresses_with_failover.back();

			for (Poco::Util::AbstractConfiguration::Keys::const_iterator jt = replica_keys.begin(); jt != replica_keys.end(); ++jt)
			{
				if (0 == strncmp(jt->c_str(), "replica", strlen("replica")))
					replica_addresses.push_back(Address(config_prefix + *it + "." + *jt));
				else
					throw Exception("Unknown element in config: " + *jt, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
			}
		}
		else
			throw Exception("Unknown element in config: " + *it, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
	}

	if (!addresses_with_failover.empty() && !addresses.empty())
			throw Exception("There must be either 'node' or 'shard' elements in config", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

		if (addresses_with_failover.size())
		{
			for (AddressesWithFailover::const_iterator it = addresses_with_failover.begin(); it != addresses_with_failover.end(); ++it)
			{
				ConnectionPools replicas;
				replicas.reserve(it->size());

				bool has_local_replics = false;
				for (Addresses::const_iterator jt = it->begin(); jt != it->end(); ++jt)
				{
					if (isLocal(*jt))
					{
						has_local_replics = true;
						break;
					}
					else
					{
						replicas.push_back(new ConnectionPool(
							settings.distributed_connections_pool_size,
							jt->host_port.host().toString(), jt->host_port.port(), "", jt->user, jt->password, data_type_factory, "server", Protocol::Compression::Enable,
							saturation(settings.connect_timeout_with_failover_ms, settings.limits.max_execution_time),
							saturation(settings.receive_timeout, settings.limits.max_execution_time),
							saturation(settings.send_timeout, settings.limits.max_execution_time)));
					}
				}

				if (has_local_replics)
					++local_nodes_num;
				else
					pools.push_back(new ConnectionPoolWithFailover(replicas, settings.connections_with_failover_max_tries));
			}
		}
		else if (addresses.size())
		{
			for (Addresses::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
			{
				if (isLocal(*it))
				{
					++local_nodes_num;
				}
				else
				{
					pools.push_back(new ConnectionPool(
						settings.distributed_connections_pool_size,
						it->host_port.host().toString(), it->host_port.port(), "", it->user, it->password, data_type_factory, "server", Protocol::Compression::Enable,
						saturation(settings.connect_timeout, settings.limits.max_execution_time),
						saturation(settings.receive_timeout, settings.limits.max_execution_time),
						saturation(settings.send_timeout, settings.limits.max_execution_time)));
				}
			}
		}
		else
			throw Exception("No addresses listed in config", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
}

Poco::Timespan Cluster::saturation(const Poco::Timespan & v, const Poco::Timespan & limit)
{
	if (limit.totalMicroseconds() == 0)
		return v;
	else
		return v > limit ? limit : v;
}

static bool interfaceEqual(const Poco::Net::NetworkInterface & interface, Poco::Net::IPAddress & address)
{
	return interface.address() == address;
}

bool Cluster::isLocal(const Address & address)
{
	///	Если среди реплик существует такая, что:
	/// - её порт совпадает с портом, который слушает сервер;
	/// - её хост резолвится в набор адресов, один из которых совпадает с одним из адресов сетевых интерфейсов сервера
	/// то нужно всегда ходить на этот шард без межпроцессного взаимодействия
	UInt16 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);
	Poco::Net::NetworkInterface::NetworkInterfaceList interfaces = Poco::Net::NetworkInterface::list();

	if (clickhouse_port == address.host_port.port() &&
		interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
										 boost::bind(interfaceEqual, _1, address.host_port.host())))
	{
		LOG_INFO(&Poco::Util::Application::instance().logger(), "Replica with address " << address.host_port.toString() << " will be processed as local.");
		return true;
	}
	return false;
}

}
