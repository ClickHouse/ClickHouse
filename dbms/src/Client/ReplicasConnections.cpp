#include <DB/Client/ReplicasConnections.h>

namespace DB
{
	ReplicasConnections::ReplicasConnections(IConnectionPool * pool_, Settings * settings_, size_t timeout_microseconds_) :
		pool(pool_),
		settings(settings_),
		timeout_microseconds(timeout_microseconds_)
	{
        auto entries = pool->getMany(settings);
		for (auto & entry : entries)
		{
			Connection * connection = &*entry;
			connections.insert(std::make_pair(connection->socket, ConnectionInfo(connection)));
		}
	}

	int ReplicasConnections::waitForReadEvent()
	{
		Poco::Net::Socket::SocketList read_list(connections.size());

		auto it = read_list.begin();
        for (auto & e : connections) 
		{
            ConnectionInfo & info = e.second;
			info.can_read = false;
			if (info.is_valid)
			{
				*it = e.first;
				++it;
			}
        }

        if (read_list.empty())
			return 0;

        int n = Poco::Net::Socket::select(read_list, write_list, except_list, Poco::Timespan(timeout_microseconds));

        for (const auto & socket : read_list) 
		{
			auto place = connections.find(socket);
			ConnectionInfo & info = place->second;
			info.can_read = true;
        }

        return n;
	}

	ReplicasConnections::ConnectionInfo & ReplicasConnections::pickConnection()
	{
        int n = waitForReadEvent();
        if (n == 0)
            throw Exception("", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);

        int max_packet_number = -1;
		ConnectionInfo * res = nullptr;

        for (auto & e : connections) {
			ConnectionInfo & info = e.second;
            if (info.can_read && 
				(info.packet_number > max_packet_number)) {
                max_packet_number = info.packet_number;
				res = &info;
            }
        }
        if (res == nullptr)
			throw Exception("", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);

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
						++info.packet_number;
						break;

					default:
						info.is_valid = false;
						break;
				}
			}
		}
	}

	void ReplicasConnections::sendQuery(const String & query, const String & query_id, UInt64 stage, 
				   const Settings * settings_, bool with_pending_data)
	{
		for (auto & e : connections)
		{
			Connection * connection = e.second.connection;
			connection->sendQuery(query, query_id, stage, settings_, with_pending_data);
		}
	}
}
