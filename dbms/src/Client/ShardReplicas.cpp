#include <DB/Client/ShardReplicas.h>

namespace DB
{
	ShardReplicas::ShardReplicas(std::vector<ConnectionPool::Entry> & entries, const Settings & settings_) :
		settings(settings_)
	{
		replica_hash.reserve(entries.size());

		for (auto & entry : entries)
		{
			Connection * connection = &*entry;
			replica_hash.insert(std::make_pair(connection->socket.impl()->sockfd(), connection));
		}
	}

	Connection * ShardReplicas::waitForReadEvent()
	{
		Poco::Net::Socket::SocketList write_list;
		Poco::Net::Socket::SocketList except_list;

		Poco::Net::Socket::SocketList read_list;
		read_list.reserve(replica_hash.size());

		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			read_list.push_back(connection->socket);
		}

		int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings.poll_interval * 1000000);
		if (n == 0)
			return nullptr;

		auto & socket = read_list[rand() % n];
		auto it = replica_hash.find(socket.impl()->sockfd());
		if (it == replica_hash.end())
			throw Exception("Unexpected replica", ErrorCodes::UNEXPECTED_REPLICA);
		return it->second;
	}

	Connection::Packet ShardReplicas::receivePacket()
	{
		Connection * connection = waitForReadEvent();
		if (connection == nullptr)
			throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

		Connection::Packet packet = connection->receivePacket();
		return packet;
	}

	void ShardReplicas::sendQuery(const String & query, const String & query_id, UInt64 stage, bool with_pending_data)
	{
		Settings query_settings = settings;
		query_settings.parallel_replicas_count = replica_hash.size();
		UInt64 offset = 0;

		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			connection->sendQuery(query, query_id, stage, &query_settings, with_pending_data);
			query_settings.parallel_replica_offset = offset;
			++offset;
		}
	}

	void ShardReplicas::disconnect()
	{
		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			connection->disconnect();
		}
	}

	void ShardReplicas::sendCancel()
	{
		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			connection->sendCancel();
		}
	}

	void ShardReplicas::drainResidualPackets()
	{
		bool caught_exceptions = false;

		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			bool again = true;

			while (again)
			{
				Connection::Packet packet = connection->receivePacket();

				switch (packet.type)
				{
					case Protocol::Server::Data:
					case Protocol::Server::Progress:
					case Protocol::Server::ProfileInfo:
					case Protocol::Server::Totals:
					case Protocol::Server::Extremes:
						break;

					case Protocol::Server::EndOfStream:
						again = false;
						continue;

					case Protocol::Server::Exception:
						// XXX Что делать?
						caught_exceptions = true;
						again = false;
						continue;

					default:
						// XXX Что делать?
						caught_exceptions = true;
						again = false;
						continue;
				}
			}
		}

		if (caught_exceptions)
		{
			// XXX Что выкидываем?
		}
	}

	std::string ShardReplicas::dumpAddresses() const
	{
		std::ostringstream os;
		for (auto & e : replica_hash)
		{
			char prefix = '\0';
			const Connection * connection = e.second;
			os << prefix << connection->getServerAddress();
			if (prefix == '\0')
				prefix = ';';
		}

		return os.str();
	}

	void ShardReplicas::sendExternalTablesData(std::vector<ExternalTablesData> & data)
	{
		if (data.size() != replica_hash.size())
			throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

		auto it = data.begin();
		for (auto & e : replica_hash)
		{
			Connection * connection = e.second;
			connection->sendExternalTablesData(*it);
			++it;
		}
	}
}
