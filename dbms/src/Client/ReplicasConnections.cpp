#include <DB/Client/ReplicasConnections.h>
#include <DB/Client/ConnectionPool.h>

namespace DB
{
	ReplicasConnections::ReplicasConnections(IConnectionPool * pool_, Settings * settings_) :
		settings(settings_),
		select_timeout(settings->poll_interval * 1000000)
	{
        auto entries = pool_->getMany(settings);
		valid_connections_count = entries.size();
		connection_hash.reserve(valid_connections_count);

		for (auto & entry : entries)
		{
			Connection * connection = &*entry;
			connection_hash.insert(std::make_pair(connection->socket.impl()->sockfd(), ConnectionInfo(connection)));
		}
	}

	int ReplicasConnections::waitForReadEvent()
	{
		if (valid_connections_count == 0)
			return 0;

		Poco::Net::Socket::SocketList read_list;
		read_list.reserve(valid_connections_count);

		for (auto & e : connection_hash)
		{
			ConnectionInfo & info = e.second;
			info.can_read = false;
			if (info.is_valid)
				read_list.push_back(info.connection->socket);
		}

        int n = Poco::Net::Socket::select(read_list, write_list, except_list, select_timeout);

        for (const auto & socket : read_list) 
		{
			auto it = connection_hash.find(socket.impl()->sockfd());
			if (it == connection_hash.end())
				throw Exception("Unexpected replica", ErrorCodes::UNEXPECTED_REPLICA);
			ConnectionInfo & info = it->second;
			info.can_read = true;
        }

        return n;
	}

	ReplicasConnections::ConnectionInfo & ReplicasConnections::pickConnection()
	{
		ConnectionInfo * res = nullptr;

        int n = waitForReadEvent();
        if (n > 0)
		{
			int max_packet_number = -1;
			for (auto & e : connection_hash) 
			{
				ConnectionInfo & info = e.second;
				if (info.can_read && (info.packet_number > max_packet_number))
				{
					max_packet_number = info.packet_number;
					res = &info;
				}
			}
		}

		if (res == nullptr)
			throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

        return *res;
	}

	Connection::Packet ReplicasConnections::receivePacket()
	{
		while (true)
		{
			ConnectionInfo & info = pickConnection();

			while (info.is_valid)
			{
				Connection::Packet packet = info.connection->receivePacket();

				if (info.packet_number == next_packet_number)
				{
					++info.packet_number;
					++next_packet_number;
					return packet;
				}

				switch (packet.type)
				{
					case Protocol::Server::Data:
					case Protocol::Server::Progress:
					case Protocol::Server::ProfileInfo:
					case Protocol::Server::Totals:
					case Protocol::Server::Extremes:
					case Protocol::Server::Exception:
						++info.packet_number;
						break;

					default:
						info.is_valid = false;
						--valid_connections_count;
						break;
				}
			}
		}
	}

	void ReplicasConnections::sendQuery(const String & query, const String & query_id, UInt64 stage, 
				   const Settings * settings_, bool with_pending_data)
	{
		for (auto & e : connection_hash)
		{
			Connection * connection = e.second.connection;
			connection->sendQuery(query, query_id, stage, settings_, with_pending_data);
		}
	}
}
