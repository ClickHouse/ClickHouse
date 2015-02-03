#pragma once

#include <DB/Client/Connection.h>
#include <DB/Client/ConnectionPool.h>

namespace DB
{
	/**
	  * Множество реплик одного шарда.
	  */
	class ShardReplicas final
	{
	public:
		ShardReplicas(std::vector<ConnectionPool::Entry> & entries, const Settings & settings_);

		~ShardReplicas() = default;

		ShardReplicas(const ShardReplicas &) = delete;
		ShardReplicas & operator=(const ShardReplicas &) = delete;

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
		Connection::Packet drain();

		/// Получить адреса реплик в виде строки.
		std::string dumpAddresses() const;

		/// Возвращает количесто реплик.
		size_t size() const { return replica_hash.size(); }

	private:
		/// Проверить, есть ли данные, которые можно прочитать на каких-нибудь репликах.
		/// Возвращает соединение на такую реплику, если оно найдётся.
		Connection ** waitForReadEvent();

	private:
		/// Реплики хэшированные по id сокета
		using ReplicaHash = std::unordered_map<int, Connection *>;

	private:
		const Settings & settings;
		ReplicaHash replica_hash;
		size_t active_connection_count = 0;
		bool sent_query = false;
		bool cancelled = false;
	};
}
