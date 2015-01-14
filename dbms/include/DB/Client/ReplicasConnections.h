#pragma once

#include <DB/Client/Connection.h>
#include <DB/Client/ConnectionPool.h>

namespace DB
{
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
		ReplicasConnections(IConnectionPool * pool_, Settings * settings_, size_t timeout_microseconds_ = 0);

		~ReplicasConnections() = default;

		ReplicasConnections(const ReplicasConnections &) = delete;

		ReplicasConnections & operator=(const ReplicasConnections &) = delete;

		int waitForReadEvent();

		ConnectionInfo & pickConnection();

		Connection::Packet receivePacket();

		void sendQuery(const String & query, const String & query_id = "", UInt64 stage = QueryProcessingStage::Complete,
					   const Settings * settings_ = nullptr, bool with_pending_data = false);

	private:
		using Connections = std::map<Poco::Net::StreamSocket, ConnectionInfo>;

	private:
		IConnectionPool * pool;
		Settings * settings;
		size_t timeout_microseconds;
		Poco::Net::Socket::SocketList write_list;
		Poco::Net::Socket::SocketList except_list;
		Connections connections;
		int next_packet_number = 0;
	};
}
