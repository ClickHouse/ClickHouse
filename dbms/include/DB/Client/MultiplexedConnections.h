#pragma once

#include <DB/Common/Throttler.h>
#include <DB/Client/Connection.h>
#include <DB/Client/ConnectionPool.h>
#include <Poco/ScopedLock.h>
#include <Poco/Mutex.h>

namespace DB
{


/** Для получения данных сразу из нескольких реплик (соединений) из одного или нексольких шардов
  * в рамках одного потока. В качестве вырожденного случая, может также работать с одним соединением.
  * Предполагается, что все функции кроме sendCancel всегда выполняются в одном потоке.
  *
  * Интерфейс почти совпадает с Connection.
  */
class MultiplexedConnections final : private boost::noncopyable
{
public:
	/// Принимает готовое соединение.
	MultiplexedConnections(Connection * connection_, const Settings * settings_, ThrottlerPtr throttler_);

	/** Принимает пул, из которого нужно будет достать одно или несколько соединений.
	  * Если флаг append_extra_info установлен, к каждому полученному блоку прилагается
	  * дополнительная информация.
	  * Если флаг get_all_replicas установлен, достаются все соединения.
	  */
	MultiplexedConnections(IConnectionPool * pool_, const Settings * settings_, ThrottlerPtr throttler_,
		bool append_extra_info = false, bool do_broadcast = false);

	/** Принимает пулы, один для каждого шарда, из которих нужно будет достать одно или несколько
	  * соединений.
	  * Если флаг append_extra_info установлен, к каждому полученному блоку прилагается
	  * дополнительная информация.
	  * Если флаг do_broadcast установлен, достаются все соединения.
	  */
	MultiplexedConnections(ConnectionPools & pools_, const Settings * settings_, ThrottlerPtr throttler_,
		bool append_extra_info = false, bool do_broadcast = false);

	/// Отправить на реплики всё содержимое внешних таблиц.
	void sendExternalTablesData(std::vector<ExternalTablesData> & data);

	/// Отправить запрос на реплики.
	void sendQuery(const String & query, const String & query_id = "",
		UInt64 stage = QueryProcessingStage::Complete, bool with_pending_data = false);

	/// Получить пакет от какой-нибудь реплики.
	Connection::Packet receivePacket();

	/// Получить информацию про последний полученный пакет.
	BlockExtraInfo getBlockExtraInfo() const;

	/// Разорвать все действующие соединения.
	void disconnect();

	/// Отправить на реплики просьбу отменить выполнение запроса
	void sendCancel();

	/** На каждой реплике читать и пропускать все пакеты до EndOfStream или Exception.
	  * Возвращает EndOfStream, если не было получено никакого исключения. В противном
	  * случае возвращает последний полученный пакет типа Exception.
	  */
	Connection::Packet drain();

	/// Получить адреса реплик в виде строки.
	std::string dumpAddresses() const;

	/// Возвращает количесто реплик.
	/// Без блокировки, потому что sendCancel() не меняет это количество.
	size_t size() const { return replica_map.size(); }

	/// Проверить, есть ли действительные реплики.
	/// Без блокировки, потому что sendCancel() не меняет состояние реплик.
	bool hasActiveConnections() const { return active_connection_total_count > 0; }

private:
	/// Соединения 1-го шарда, затем соединения 2-го шарда, и т.д.
	using Connections = std::vector<Connection *>;

	/// Состояние соединений одного шарда.
	struct ShardState
	{
		/// Количество выделенных соединений, т.е. реплик, для этого шарда.
		size_t allocated_connection_count;
		/// Текущее количество действительных соединений к репликам этого шарда.
		size_t active_connection_count;
	};

	/// Описание одной реплики.
	struct ReplicaState
	{
		/// Индекс соединения.
		size_t connection_index;
		/// Владелец этой реплики.
		ShardState * shard_state;
	};

	/// Реплики хэшированные по id сокета.
	using ReplicaMap = std::unordered_map<int, ReplicaState>;

	/// Состояние каждого шарда.
	using ShardStates = std::vector<ShardState>;

private:
	void initFromShard(IConnectionPool * pool);

	/// Зарегистрировать шарды.
	void registerShards();

	/// Зарегистрировать реплики одного шарда.
	void registerReplicas(size_t index_begin, size_t index_end, ShardState & shard_state);

	/// Внутренняя версия функции receivePacket без блокировки.
	Connection::Packet receivePacketUnlocked();

	/// Внутренняя версия функции dumpAddresses без блокировки.
	std::string dumpAddressesUnlocked() const;

	/// Получить реплику, на которой можно прочитать данные.
	ReplicaMap::iterator getReplicaForReading();

	/** Проверить, есть ли данные, которые можно прочитать на каких-нибудь репликах.
	  * Возвращает одну такую реплику, если она найдётся.
	  */
	ReplicaMap::iterator waitForReadEvent();

	/// Пометить реплику как недействительную.
	void invalidateReplica(ReplicaMap::iterator it);

private:
	const Settings * settings;

	Connections connections;
	ReplicaMap replica_map;
	ShardStates shard_states;

	/// Если не nullptr, то используется, чтобы ограничить сетевой трафик.
	ThrottlerPtr throttler;

	std::vector<ConnectionPool::Entry> pool_entries;

	/// Соединение, c которого был получен последний блок.
	Connection * current_connection;
	/// Информация про последний полученный блок, если поддерживается.
	std::unique_ptr<BlockExtraInfo> block_extra_info;

	/// Текущее количество действительных соединений к репликам.
	size_t active_connection_total_count = 0;
	/// Запрос выполняется параллельно на нескольких репликах.
	bool supports_parallel_execution;
	/// Отправили запрос
	bool sent_query = false;
	/// Отменили запрос
	bool cancelled = false;

	bool do_broadcast = false;

	/// Мьютекс для того, чтобы функция sendCancel могла выполняться безопасно
	/// в отдельном потоке.
	mutable Poco::FastMutex cancel_mutex;
};

}
