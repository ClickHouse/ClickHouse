#include <DB/Client/ParallelReplicas.h>

namespace DB
{

ParallelReplicas::ParallelReplicas(Connection * connection_, Settings * settings_, ThrottlerPtr throttler_)
	: settings(settings_), throttler(throttler_),
	active_replica_count(1),
	supports_parallel_execution(false)
{
	registerReplica(connection_);
}

ParallelReplicas::ParallelReplicas(IConnectionPool * pool_, Settings * settings_, ThrottlerPtr throttler_)
	: settings(settings_), throttler(throttler_)
{
	if (pool_ == nullptr)
		throw Exception("Null pool specified", ErrorCodes::LOGICAL_ERROR);

	bool has_many_replicas = (settings != nullptr) && (settings->max_parallel_replicas > 1);
	if (has_many_replicas)
	{
		pool_entries = pool_->getMany(settings);
		active_replica_count = pool_entries.size();
		supports_parallel_execution = (active_replica_count > 1);

		if (active_replica_count == 0)
			throw Exception("No connection available", ErrorCodes::LOGICAL_ERROR);

		replica_map.reserve(active_replica_count);
		for (auto & entry : pool_entries)
			registerReplica(&*entry);
	}
	else
	{
		active_replica_count = 1;
		supports_parallel_execution = false;

		pool_entry = pool_->get(settings);
		registerReplica(&*pool_entry);
	}
}

void ParallelReplicas::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (!sent_query)
		throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

	if (data.size() < active_replica_count)
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
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (sent_query)
		throw Exception("Query already sent.", ErrorCodes::LOGICAL_ERROR);

	if (supports_parallel_execution)
	{
		Settings query_settings = *settings;
		query_settings.parallel_replicas_count = active_replica_count;
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

		if (offset > 0)
			sent_query = true;
	}
	else
	{
		auto it = replica_map.begin();
		Connection * connection = it->second;
		if (connection != nullptr)
		{
			connection->sendQuery(query, query_id, stage, settings, with_pending_data);
			sent_query = true;
		}
	}
}

Connection::Packet ParallelReplicas::receivePacket()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);
	return receivePacketUnlocked();
}

void ParallelReplicas::disconnect()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	for (auto it = replica_map.begin(); it != replica_map.end(); ++it)
	{
		Connection * connection = it->second;
		if (connection != nullptr)
		{
			connection->disconnect();
			invalidateReplica(it);
		}
	}
}

void ParallelReplicas::sendCancel()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

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
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (!cancelled)
		throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

	Connection::Packet res;
	res.type = Protocol::Server::EndOfStream;

	while (hasActiveReplicas())
	{
		Connection::Packet packet = receivePacketUnlocked();

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
				/// Если мы получили исключение или неизвестный пакет, сохраняем его.
				res = packet;
				break;
		}
	}

	return res;
}

std::string ParallelReplicas::dumpAddresses() const
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	bool is_first = true;
	std::ostringstream os;
	for (auto & e : replica_map)
	{
		const Connection * connection = e.second;
		if (connection != nullptr)
		{
			os << (is_first ? "" : "; ") << connection->getServerAddress();
			is_first = false;
		}
	}

	return os.str();
}

void ParallelReplicas::registerReplica(Connection * connection)
{
	if (connection == nullptr)
		throw Exception("Invalid connection specified in parameter.", ErrorCodes::LOGICAL_ERROR);
	auto res = replica_map.insert(std::make_pair(connection->socket.impl()->sockfd(), connection));
	if (!res.second)
		throw Exception("Invalid set of connections.", ErrorCodes::LOGICAL_ERROR);

	if (throttler)
		connection->setThrottler(throttler);
}

Connection::Packet ParallelReplicas::receivePacketUnlocked()
{
	if (!sent_query)
		throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
	if (!hasActiveReplicas())
		throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

	auto it = getReplicaForReading();
	if (it == replica_map.end())
		throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

	Connection * connection = it->second;
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
			invalidateReplica(it);
			break;

		case Protocol::Server::Exception:
		default:
			connection->disconnect();
			invalidateReplica(it);
			break;
	}

	return packet;
}

ParallelReplicas::ReplicaMap::iterator ParallelReplicas::getReplicaForReading()
{
	ReplicaMap::iterator it;

	if (supports_parallel_execution)
		it = waitForReadEvent();
	else
	{
		it = replica_map.begin();
		if (it->second == nullptr)
			it = replica_map.end();
	}

	return it;
}

ParallelReplicas::ReplicaMap::iterator ParallelReplicas::waitForReadEvent()
{
	Poco::Net::Socket::SocketList read_list;
	read_list.reserve(active_replica_count);

	/** Сначала проверяем, есть ли данные, которые уже лежат в буфере
		* хоть одного соединения.
		*/
	for (auto & e : replica_map)
	{
		Connection * connection = e.second;
		if ((connection != nullptr) && connection->hasReadBufferPendingData())
			read_list.push_back(connection->socket);
	}

	/** Если не было найдено никаких данных, то проверяем, есть ли соединения
		* готовые для чтения.
		*/
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
		int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings->poll_interval * 1000000);
		if (n == 0)
			return replica_map.end();
	}

	auto & socket = read_list[rand() % read_list.size()];
	return replica_map.find(socket.impl()->sockfd());
}

void ParallelReplicas::invalidateReplica(ParallelReplicas::ReplicaMap::iterator it)
{
	it->second = nullptr;
	--active_replica_count;
}

}
