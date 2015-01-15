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
		ShardReplicas(std::vector<ConnectionPool::Entry> & entries, Settings * settings_);

		~ShardReplicas() = default;

		ShardReplicas(const ShardReplicas &) = delete;
		ShardReplicas & operator=(const ShardReplicas &) = delete;

		/// Получить пакет от какой-нибудь реплики.
		Connection::Packet receivePacket();

		/// Отправить запрос ко всем репликам.
		void sendQuery(const String & query, const String & query_id = "", UInt64 stage = QueryProcessingStage::Complete,
					   const Settings * settings_ = nullptr, bool with_pending_data = false);

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
		/// Описание реплики.
		struct Replica
		{
			Replica(Connection * connection_) : connection(connection_) {}

			/// Соединение к реплике
			Connection * connection;

			/// Номер следующего ожидаемого пакета.
			int next_packet_number = 0;

			/// Есть ли данные, которые можно прочитать?
			bool can_read = false;

			/// Является ли реплика валидной для чтения?
			bool is_valid = true;
		};

		/// Реплики хэшированные по id сокета
		using ReplicaHash = std::unordered_map<int, Replica>;

	private:
		/// Выбрать реплику, на которой можно прочитать данные.
		Replica & pickReplica();

		/// Проверить, есть ли данные, которые можно прочитать на каких-нибудь репликах.
		int waitForReadEvent();

	private:
		Settings * settings;

		ReplicaHash replica_hash;
		size_t valid_replicas_count;

		/// Номер следующего ожидаемого пакета.
		int next_packet_number = 0;
	};
}
