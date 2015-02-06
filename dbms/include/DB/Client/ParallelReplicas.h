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
		/// Принимает готовое соединение.
		ParallelReplicas(Connection * connection_, Settings * settings_);

		/// Принимает пул, из которого нужно будет достать одно или несколько соединений.
		ParallelReplicas(IConnectionPool * pool_, Settings * settings_);

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

		/// Проверить, есть ли действительные соединения к репликам.
		bool hasActiveConnections() const { return active_connection_count > 0; }

	private:
		/// Реплики хэшированные по id сокета
		using ReplicaMap = std::unordered_map<int, Connection *>;

	private:
		/// Зарегистрировать соединение к реплике.
		void addConnection(Connection * connection);

		/// Получить соединение к реплике, на которой можно прочитать данные.
		ReplicaMap::iterator getConnection();

		/// Проверить, есть ли данные, которые можно прочитать на каких-нибудь репликах.
		/// Возвращает соединение к такой реплике, если оно найдётся.
		ReplicaMap::iterator waitForReadEvent();

		// Пометить соединение как недействительное.
		void invalidateConnection(ReplicaMap::iterator it);

	private:
		Settings * settings;
		ReplicaMap replica_map;

		std::vector<ConnectionPool::Entry> pool_entries;
		ConnectionPool::Entry pool_entry;

		/// Текущее количество действительных соединений к репликам.
		size_t active_connection_count = 0;
		/// Запрос выполняется параллельно на нескольких репликах.
		bool supports_parallel_execution;
		/// Отправили запрос
		bool sent_query = false;
		/// Отменили запрос
		bool cancelled = false;
	};
}
