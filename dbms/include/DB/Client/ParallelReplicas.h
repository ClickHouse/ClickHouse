#pragma once

#include <DB/Client/Connection.h>
#include <DB/Client/ConnectionPool.h>

namespace DB
{
	/**
	  * Множество реплик одного шарда.
	  */
	class ParallelReplicas final
	{
	public:
		ParallelReplicas(Connection * connection_, const Settings * settings_);
		ParallelReplicas(std::vector<ConnectionPool::Entry> & entries, const Settings * settings_);

		ParallelReplicas(const ParallelReplicas &) = delete;
		ParallelReplicas & operator=(const ParallelReplicas &) = delete;

		/// Отправить на реплики всё содержимое внешних таблиц.
		void sendExternalTablesData(std::vector<ExternalTablesData> & data);

		/// Отправить запрос на реплики.
		void sendQuery(const String & query, const String & query_id = "",
					   UInt64 stage = QueryProcessingStage::Complete, bool with_pending_data = false);

		/// Получить пакет от какой-нибудь реплики.
		Connection::Packet receivePacket();

		/// Разорвать соединения к репликам
		void disconnect();

		/// Отменить запросы к репликам
		void sendCancel();

		/// Для каждой реплики получить оставшиеся пакеты после отмена запроса.
		/// Возвращает либо последнее полученное исключение либо пакет EndOfStream.
		Connection::Packet drain();

		/// Получить адреса реплик в виде строки.
		std::string dumpAddresses() const;

		/// Возвращает количесто реплик.
		size_t size() const { return replica_map.size(); }

		bool hasActiveConnections() const { return active_connection_count > 0; }

	private:
		/// Реплики хэшированные по id сокета
		using ReplicaMap = std::unordered_map<int, Connection *>;

	private:
		/// Добавить соединение к реплике.
		void addConnection(Connection * connection);

		ReplicaMap::iterator getConnection();

		/// Проверить, есть ли данные, которые можно прочитать на каких-нибудь репликах.
		/// Возвращает соединение на такую реплику, если оно найдётся.
		ReplicaMap::iterator waitForReadEvent();

		void invalidateConnection(ReplicaMap::iterator it);

	private:
		const Settings * settings;
		ReplicaMap replica_map;
		size_t active_connection_count = 0;
		bool supports_parallel_execution;
		bool sent_query = false;
		bool cancelled = false;
	};
}
