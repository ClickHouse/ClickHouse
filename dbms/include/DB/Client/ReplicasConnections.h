#pragma once

#include <DB/Client/Connection.h>

namespace DB
{
	class IConnectionPool;
	
	class ReplicasConnections
	{
	public:
		struct ConnectionInfo
		{
			ConnectionInfo(Connection * connection_) : connection(connection_) {}
			Connection * connection;
			int packet_number = 0;
			bool can_read = false;
			bool is_valid = true;
		};

	public:
		ReplicasConnections(IConnectionPool * pool_, Settings * settings_);

		~ReplicasConnections() = default;

		ReplicasConnections(const ReplicasConnections &) = delete;

		ReplicasConnections & operator=(const ReplicasConnections &) = delete;

		int waitForReadEvent();

		ConnectionInfo & pickConnection();

		Connection::Packet receivePacket();

		void sendQuery(const String & query, const String & query_id = "", UInt64 stage = QueryProcessingStage::Complete,
					   const Settings * settings_ = nullptr, bool with_pending_data = false);

		void disconnect();

		void sendCancel();

		void drainResidualPackets();

		std::string dumpAddresses() const;

		size_t size() const;

		void sendExternalTablesData(std::vector<ExternalTablesData> & data);

	private:
		using ConnectionHash = std::unordered_map<int, ConnectionInfo>;

	private:
		Settings * settings;
		Poco::Timespan select_timeout;
		ConnectionHash connection_hash;
		size_t valid_connections_count;
		int next_packet_number = 0;
		Poco::Net::Socket::SocketList write_list;
		Poco::Net::Socket::SocketList except_list;
	};
}
