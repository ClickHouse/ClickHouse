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

		/// Получить пакет от какой-нибудь реплики.
		Connection::Packet receivePacket();

		/// Отправить запрос ко всем репликам.
		void sendQuery(const String & query, const String & query_id = "",
					   UInt64 stage = QueryProcessingStage::Complete, bool with_pending_data = false);

		/// Разорвать соединения ко всем репликам
		void disconnect();

		/// Отменить запросы у всех реплик
		void sendCancel();

		/// Для каждой реплики получить оставшиеся пакеты при отмене запроса.
		void drainResidualPackets();

		/// Получить адреса всех реплик в виде строки.
		std::string dumpAddresses() const;

		/// Возвращает количесто реплик.
		size_t size() const 
		{
			return replica_hash.size();
		}

		/// Отправить ко всем репликам всё содержимое внешних таблиц.
		void sendExternalTablesData(std::vector<ExternalTablesData> & data);

	private:
		/// Проверить, есть ли данные, которые можно прочитать на каких-нибудь репликах.
		/// Возвращает соединение на реплику, с которой можно прочитать данные, если такая есть.
		Connection * waitForReadEvent();

	private:
		/// Реплики хэшированные по id сокета
		using ReplicaHash = std::unordered_map<int, Connection *>;

	private:
		const Settings & settings;
		ReplicaHash replica_hash;
	};
}
