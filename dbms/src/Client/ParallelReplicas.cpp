#include <DB/Client/ParallelReplicas.h>
#include <boost/concept_check.hpp>

namespace DB
{
	ParallelReplicas::ParallelReplicas(std::vector<ConnectionPool::Entry> & entries, const Settings & settings_) :
		settings(settings_),
		active_connection_count(entries.size())
	{
		replica_map.reserve(entries.size());

		for (auto & entry : entries)
		{
			Connection * connection = &*entry;
			if (connection == nullptr)
				throw Exception("Invalid connection specified in parameter.", ErrorCodes::LOGICAL_ERROR);
			auto res = replica_map.insert(std::make_pair(connection->socket.impl()->sockfd(), connection));
			if (!res.second)
				throw Exception("Invalid set of connections.", ErrorCodes::LOGICAL_ERROR);
		}
	}

	void ParallelReplicas::sendExternalTablesData(std::vector<ExternalTablesData> & data)
	{
		if (!sent_query)
			throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

		if (data.size() < active_connection_count)
			throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

		auto it = data.begin();
		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				connection->sendExternalTablesData(*it);
			++it;
		}
	}

	void ParallelReplicas::sendQuery(const String & query, const String & query_id, UInt64 stage, bool with_pending_data)
	{
		if (sent_query)
			throw Exception("Query already sent.", ErrorCodes::LOGICAL_ERROR);

		Settings query_settings = settings;
		query_settings.parallel_replicas_count = replica_map.size();
		UInt64 offset = 0;

		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
			{
				query_settings.parallel_replica_offset = offset;
				connection->sendQuery(query, query_id, stage, &query_settings, with_pending_data);
				++offset;
			}
		}

		sent_query = true;
	}

	Connection::Packet ParallelReplicas::receivePacket()
	{
		if (!sent_query)
			throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
		if (!hasActiveConnections())
			throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

		auto it = waitForReadEvent();
		if (it == replica_map.end())
			throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

		Connection * & connection = it->second;
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
			case Protocol::Server::Exception:
			default:
				connection = nullptr;
				--active_connection_count;
				break;
		}

		return packet;
	}

	void ParallelReplicas::disconnect()
	{
		for (auto & e : replica_map)
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

	void ParallelReplicas::sendCancel()
	{
		if (!sent_query || cancelled)
			throw Exception("Cannot cancel. Either no query sent or already cancelled.", ErrorCodes::LOGICAL_ERROR);

		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				connection->sendCancel();
		}

		cancelled = true;
	}

	Connection::Packet ParallelReplicas::drain()
	{
		if (!cancelled)
			throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

		Connection::Packet res;
		res.type = Protocol::Server::EndOfStream;

		while (hasActiveConnections())
		{
			Connection::Packet packet = receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
				case Protocol::Server::Progress:
				case Protocol::Server::ProfileInfo:
				case Protocol::Server::Totals:
				case Protocol::Server::Extremes:
				case Protocol::Server::EndOfStream:
					break;

				case Protocol::Server::Exception:
				default:
					res = packet;
					break;
			}
		}

		return res;
	}

	std::string ParallelReplicas::dumpAddresses() const
	{
		bool is_first = true;
		std::ostringstream os;
		for (auto & e : replica_map)
		{
			const Connection * connection = e.second;
			if (connection != nullptr)
			{
				os << (is_first ? "" : "; ") << connection->getServerAddress();
				if (is_first) { is_first = false; }
			}
		}

		return os.str();
	}

	ParallelReplicas::ReplicaMap::iterator ParallelReplicas::waitForReadEvent()
	{
		Poco::Net::Socket::SocketList read_list;
		read_list.reserve(active_connection_count);

		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if ((connection != nullptr) && connection->hasReadBufferPendingData())
				read_list.push_back(connection->socket);
		}

		if (read_list.empty())
		{
			Poco::Net::Socket::SocketList write_list;
			Poco::Net::Socket::SocketList except_list;

			for (auto & e : replica_map)
			{
				Connection * connection = e.second;
				if (connection != nullptr)
					read_list.push_back(connection->socket);
			}
			int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings.poll_interval * 1000000);
			if (n == 0)
				return replica_map.end();
		}

		auto & socket = read_list[rand() % read_list.size()];
		return replica_map.find(socket.impl()->sockfd());
	}
}
