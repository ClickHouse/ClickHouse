#include <map>
#include <DB/Interpreters/Settings.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/Client/ConnectionPool.h>
#pragma once

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
	Cluster(const Settings & settings, const DataTypeFactory & data_type_factory, const std::string & cluster_name);

	/// Построить кластер по именам шардов и реплик, локальные обрабатываются так же как удаленные
	Cluster(const Settings & settings, const DataTypeFactory & data_type_factory, std::vector< std::vector<String> > names,
			const String & username, const String & password);

	/// количество узлов clickhouse сервера, расположенных локально
	/// к локальным узлам обращаемся напрямую
	size_t getLocalNodesNum() const { return local_nodes_num; }

	/// Соединения с удалёнными серверами.
	ConnectionPools pools;

	/// используеться для выставления ограничения на размер таймаута
	static Poco::Timespan saturation(const Poco::Timespan & v, const Poco::Timespan & limit);

private:
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
		Poco::Net::SocketAddress host_port;
		String user;
		String password;

		Address(const std::string & config_prefix);
		Address(const Poco::Net::SocketAddress & host_port_, const String & user_, const String & password_);
	};

	bool isLocal(const Address & address);

	/// Массив шардов. Каждый шард - адреса одного сервера.
	typedef std::vector<Address> Addresses;

	/// Массив шардов. Для каждого шарда - массив адресов реплик (серверов, считающихся идентичными).
	typedef std::vector<Addresses> AddressesWithFailover;

	Addresses addresses;
	AddressesWithFailover addresses_with_failover;

	size_t local_nodes_num;
};

struct Clusters : public std::map<std::string, Cluster>
{
	Clusters(const Settings & settings, const DataTypeFactory & data_type_factory,
			 const std::string & config_name = "remote_servers");
};
}
