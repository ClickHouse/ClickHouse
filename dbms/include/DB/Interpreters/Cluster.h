#pragma once

#include <map>
#include <DB/Interpreters/Settings.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/Client/ConnectionPoolWithFailover.h>
#include <Poco/Net/SocketAddress.h>

namespace DB
{

/// Cluster содержит пулы соединений до каждого из узлов
/// С локальными узлами соединение не устанавливается, а выполяется запрос напрямую.
/// Поэтому храним только количество локальных узлов
/// В конфиге кластер включает в себя узлы <node> или <shard>
class Cluster : private boost::noncopyable
{
public:
	Cluster(const Settings & settings, const String & cluster_name);

	/// Построить кластер по именам шардов и реплик. Локальные обрабатываются так же как удаленные.
	Cluster(const Settings & settings, std::vector<std::vector<String>> names,
			const String & username, const String & password);

	/// количество узлов clickhouse сервера, расположенных локально
	/// к локальным узлам обращаемся напрямую
	size_t getLocalNodesNum() const { return local_nodes_num; }

	/// используеться для выставления ограничения на размер таймаута
	static Poco::Timespan saturate(const Poco::Timespan & v, const Poco::Timespan & limit);

public:
	/// Соединения с удалёнными серверами.
	ConnectionPools pools;

	struct ShardInfo
	{
		/// contains names of directories for asynchronous write to StorageDistributed
		std::vector<std::string> dir_names;
		UInt32 shard_num;
		int weight;
		size_t num_local_nodes;
	};
	std::vector<ShardInfo> shard_info_vec;
	std::vector<size_t> slot_to_shard;

	struct Address
	{
		/** В конфиге адреса либо находятся в узлах <node>:
		* <node>
		* 	<host>example01-01-1</host>
		* 	<port>9000</port>
		* 	<!-- <user>, <password>, если нужны -->
		* </node>
		* ...
		* либо в узлах <shard>, и внутри - <replica>
		* <shard>
		* 	<replica>
		* 		<host>example01-01-1</host>
		* 		<port>9000</port>
		* 		<!-- <user>, <password>, если нужны -->
		*	</replica>
		* </shard>
		*/
		Poco::Net::SocketAddress resolved_address;
		String host_name;
		UInt16 port;
		String user;
		String password;
		UInt32 replica_num;

		Address(const String & config_prefix);
		Address(const String & host_port_, const String & user_, const String & password_);
	};

private:
	static bool isLocal(const Address & address);

public:
	/// Массив шардов. Каждый шард - адреса одного сервера.
	typedef std::vector<Address> Addresses;

	/// Массив шардов. Для каждого шарда - массив адресов реплик (серверов, считающихся идентичными).
	typedef std::vector<Addresses> AddressesWithFailover;

public:
	const Addresses & getShardsInfo() const { return addresses; }
	const AddressesWithFailover & getShardsWithFailoverInfo() const { return addresses_with_failover; }

private:
	Addresses addresses;
	AddressesWithFailover addresses_with_failover;

	size_t local_nodes_num = 0;
};

struct Clusters
{
	typedef std::map<String, Cluster> Impl;
	Impl impl;

	Clusters(const Settings & settings, const String & config_name = "remote_servers");
};

}
