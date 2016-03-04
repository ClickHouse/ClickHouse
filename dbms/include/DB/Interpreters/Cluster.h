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
class Cluster
{
public:
	Cluster(const Settings & settings, const String & cluster_name);

	/// Построить кластер по именам шардов и реплик. Локальные обрабатываются так же как удаленные.
	Cluster(const Settings & settings, std::vector<std::vector<String>> names,
			const String & username, const String & password);

	Cluster(const Cluster &) = delete;
	Cluster & operator=(const Cluster &) = delete;

	/// используеться для выставления ограничения на размер таймаута
	static Poco::Timespan saturate(const Poco::Timespan & v, const Poco::Timespan & limit);

public:
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

	using Addresses = std::vector<Address>;
	using AddressesWithFailover = std::vector<Addresses>;

	struct ShardInfo
	{
	public:
		bool isLocal() const { return !local_addresses.empty(); }
		bool hasRemoteConnections() const { return !pool.isNull(); }
		size_t getLocalNodeCount() const { return local_addresses.size(); }

	public:
		/// contains names of directories for asynchronous write to StorageDistributed
		std::vector<std::string> dir_names;
		UInt32 shard_num;
		int weight;
		Addresses local_addresses;
		mutable ConnectionPoolPtr pool;
	};

	using ShardsInfo = std::vector<ShardInfo>;

public:
	String getName() const { return name; }
	const ShardsInfo & getShardsInfo() const { return shards_info; }
	const Addresses & getShardsAddresses() const { return addresses; }
	const AddressesWithFailover & getShardsWithFailoverAddresses() const { return addresses_with_failover; }

	const ShardInfo * getAnyRemoteShardInfo() const { return any_remote_shard_info; }

	/// Количество удалённых шардов.
	size_t getRemoteShardCount() const { return remote_shard_count; }

	/// Количество узлов clickhouse сервера, расположенных локально
	/// к локальным узлам обращаемся напрямую.
	size_t getLocalShardCount() const { return local_shard_count; }

public:
	std::vector<size_t> slot_to_shard;

private:
	void initMisc();

	/// Create a unique name based on the list of addresses and ports.
	/// We need it in order to be able to perform resharding requests
	/// on tables that have the distributed engine.
	void assignName();

private:
	/// Название кластера.
	String name;
	/// Описание шардов кластера.
	ShardsInfo shards_info;
	/// Любой удалённый шард.
	ShardInfo * any_remote_shard_info = nullptr;
	/// Массив шардов. Каждый шард - адреса одного сервера.
	Addresses addresses;
	/// Массив шардов. Для каждого шарда - массив адресов реплик (серверов, считающихся идентичными).
	AddressesWithFailover addresses_with_failover;

	size_t remote_shard_count = 0;
	size_t local_shard_count = 0;
};


class Clusters
{
public:
	Clusters(const Settings & settings, const String & config_name = "remote_servers");

	Clusters(const Clusters &) = delete;
	Clusters & operator=(const Clusters &) = delete;

public:
	using Impl = std::map<String, Cluster>;

public:
	Impl impl;
};

}
