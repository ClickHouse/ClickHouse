#include <DB/Client/ShardReplicas.h>

namespace DB
{
	ShardReplicas::ShardReplicas(std::vector<ConnectionPool::Entry> & entries, const Settings & settings_) :
		settings(settings_),
		active_connection_count(entries.size())
	{
		replica_hash.reserve(entries.size());

		for (auto & entry : entries)
		{
			Connection * connection = &*entry;
			if (connection == nullptr)
				throw Exception("Invalid connection specified in parameter.");
			auto res = replica_hash.insert(std::make_pair(connection->socket.impl()->sockfd(), connection));
			if (!res.second)
				throw Exception("Invalid set of connections.");
		}
	}

	void ShardReplicas::sendExternalTablesData(std::vector<ExternalTablesData> & data)
	{
		if (sent_query)
			throw Exception("Cannot send external tables data: query already sent.");

		if (data.size() < active_connection_count)
			throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

		auto it = data.begin();
		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				connection->sendExternalTablesData(*it);
			++it;
		}
	}

	void ShardReplicas::sendQuery(const String & query, const String & query_id, UInt64 stage, bool with_pending_data)
	{
		if (sent_query)
			throw Exception("Query already sent.");

		Settings query_settings = settings;
		query_settings.parallel_replicas_count = replica_hash.size();
		UInt64 offset = 0;

		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
			{
				connection->sendQuery(query, query_id, stage, &query_settings, with_pending_data);
				query_settings.parallel_replica_offset = offset;
				++offset;
			}
		}

		sent_query = true;
	}

	Connection::Packet ShardReplicas::receivePacket()
	{
		if (!sent_query)
			throw Exception("Cannot receive packets: no query sent.");

		Connection ** connection = waitForReadEvent();
		if (connection == nullptr)
			throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

		Connection::Packet packet = (*connection)->receivePacket();

		switch (packet.type)
		{
			case Protocol::Server::Data:
			case Protocol::Server::Progress:
			case Protocol::Server::ProfileInfo:
			case Protocol::Server::Totals:
			case Protocol::Server::Extremes:
				break;

			case Protocol::Server::EndOfStream:
				*connection = nullptr;
				--active_connection_count;
				if (active_connection_count > 0)
				{
					Connection::Packet empty_packet;
					empty_packet.type = Protocol::Server::Data;
					return empty_packet;
				}
				break;

			case Protocol::Server::Exception:
			default:
				*connection = nullptr;
				--active_connection_count;
				if (!cancelled)
				{
					sendCancel();
					(void) drain();
				}
				break;
		}

		return packet;
	}

	void ShardReplicas::disconnect()
	{
		for (auto & e : replica_hash)
		{
			Connection * & connection = e.second;
			if (connection != nullptr)
			{
				connection->disconnect();
				connection = nullptr;
				--active_connection_count;
			}
		}
	}

	void ShardReplicas::sendCancel()
	{
		if (!sent_query || cancelled)
			throw Exception("Cannot cancel. Either no query sent or already cancelled.");

		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				connection->sendCancel();
		}

		cancelled = true;
	}

	Connection::Packet ShardReplicas::drain()
	{
		if (!cancelled)
			throw Exception("Cannot drain connections: cancel first.");

		Connection::Packet res;
		res.type = Protocol::Server::EndOfStream;

		while (active_connection_count > 0)
		{
			Connection::Packet packet = receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
				case Protocol::Server::Progress:
				case Protocol::Server::ProfileInfo:
				case Protocol::Server::Totals:
				case Protocol::Server::Extremes:
					break;

				case Protocol::Server::EndOfStream:
					return res;

				case Protocol::Server::Exception:
				default:
					res = packet;
					break;
			}
		}

		return res;
	}

	std::string ShardReplicas::dumpAddresses() const
	{
		std::ostringstream os;
		for (auto & e : replica_hash)
		{
			char prefix = '\0';
			const Connection * connection = e.second;
			if (connection != nullptr)
			{
				os << prefix << connection->getServerAddress();
				if (prefix == '\0')
					prefix = ';';
			}
		}

		return os.str();
	}

	Connection ** ShardReplicas::waitForReadEvent()
	{
		Poco::Net::Socket::SocketList read_list;
		Poco::Net::Socket::SocketList write_list;
		Poco::Net::Socket::SocketList except_list;

		read_list.reserve(active_connection_count);

		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				read_list.push_back(connection->socket);
		}

		int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings.poll_interval * 1000000);
		if (n == 0)
			return nullptr;

		auto & socket = read_list[rand() % n];
		auto it = replica_hash.find(socket.impl()->sockfd());
		if (it == replica_hash.end())
			throw Exception("Unexpected replica", ErrorCodes::UNEXPECTED_REPLICA);
		return &(it->second);
	}
}
