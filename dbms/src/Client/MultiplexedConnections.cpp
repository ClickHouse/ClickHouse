#include <DB/Client/MultiplexedConnections.h>

namespace DB
{

MultiplexedConnections::MultiplexedConnections(Connection * connection_, const Settings * settings_, ThrottlerPtr throttler_)
	: settings(settings_), throttler(throttler_), supports_parallel_execution(false)
{
	if (connection_ == nullptr)
		throw Exception("Invalid connection specified", ErrorCodes::LOGICAL_ERROR);

	active_connection_total_count = 1;

	ShardState shard_state;
	shard_state.allocated_connection_count = active_connection_total_count;
	shard_state.active_connection_count = active_connection_total_count;

	shard_states.push_back(shard_state);

	ReplicaState replica_state;
	replica_state.connection_index = 0;
	replica_state.shard_state = &shard_states[0];

	connection_->setThrottler(throttler);
	connections.push_back(connection_);

	auto res = replica_map.emplace(connections[0]->socket.impl()->sockfd(), replica_state);
	if (!res.second)
		throw Exception("Invalid set of connections", ErrorCodes::LOGICAL_ERROR);
}

MultiplexedConnections::MultiplexedConnections(IConnectionPool * pool_, const Settings * settings_, ThrottlerPtr throttler_,
	bool append_extra_info, bool do_broadcast_)
	: settings(settings_), throttler(throttler_), do_broadcast(do_broadcast_)
{
	if (pool_ == nullptr)
		throw Exception("Invalid pool specified", ErrorCodes::LOGICAL_ERROR);

	initFromShard(pool_);
	registerShards();

	supports_parallel_execution = active_connection_total_count > 1;

	if (append_extra_info)
		block_extra_info.reset(new BlockExtraInfo);
}

MultiplexedConnections::MultiplexedConnections(ConnectionPools & pools_, const Settings * settings_, ThrottlerPtr throttler_,
	bool append_extra_info, bool do_broadcast_)
	: settings(settings_), throttler(throttler_), do_broadcast(do_broadcast_)
{
	if (pools_.empty())
		throw Exception("Pools are not specified", ErrorCodes::LOGICAL_ERROR);

	for (auto & pool : pools_)
	{
		if (pool.isNull())
			throw Exception("Invalid pool specified", ErrorCodes::LOGICAL_ERROR);
		initFromShard(pool.get());
	}

	registerShards();

	supports_parallel_execution = active_connection_total_count > 1;

	if (append_extra_info)
		block_extra_info.reset(new BlockExtraInfo);
}

void MultiplexedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (!sent_query)
		throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

	if (data.size() < active_connection_total_count)
		throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

	auto it = data.begin();
	for (auto & e : replica_map)
	{
		ReplicaState & state = e.second;
		Connection * connection = connections[state.connection_index];
		if (connection != nullptr)
			connection->sendExternalTablesData(*it);
		++it;
	}
}

void MultiplexedConnections::sendQuery(const String & query, const String & query_id, UInt64 stage, bool with_pending_data)
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (sent_query)
		throw Exception("Query already sent.", ErrorCodes::LOGICAL_ERROR);

	if (supports_parallel_execution)
	{
		if (settings == nullptr)
		{
			/// Каждый шард имеет один адрес.
			auto it = connections.begin();
			for (size_t i = 0; i < shard_states.size(); ++i)
			{
				Connection * connection = *it;
				if (connection == nullptr)
					throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

				connection->sendQuery(query, query_id, stage, nullptr, with_pending_data);
				++it;
			}
		}
		else
		{
			/// Каждый шард имеет одну или несколько реплик.
			auto it = connections.begin();
			for (const auto & shard_state : shard_states)
			{
				Settings query_settings = *settings;
				query_settings.parallel_replicas_count = shard_state.active_connection_count;

				UInt64 offset = 0;

				for (size_t i = 0; i < shard_state.allocated_connection_count; ++i)
				{
					Connection * connection = *it;
					if (connection == nullptr)
						throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

					query_settings.parallel_replica_offset = offset;
					connection->sendQuery(query, query_id, stage, &query_settings, with_pending_data);
					++offset;
					++it;
				}
			}
		}
	}
	else
	{
		Connection * connection = connections[0];
		if (connection == nullptr)
			throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

		connection->sendQuery(query, query_id, stage, settings, with_pending_data);
	}

	sent_query = true;
}

Connection::Packet MultiplexedConnections::receivePacket()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);
	const auto & packet = receivePacketUnlocked();
	if (block_extra_info)
	{
		if (packet.type == Protocol::Server::Data)
			current_connection->fillBlockExtraInfo(*block_extra_info);
		else
			block_extra_info->is_valid = false;
	}
	return packet;
}

BlockExtraInfo MultiplexedConnections::getBlockExtraInfo() const
{
	if (!block_extra_info)
		throw Exception("MultiplexedConnections object not configured for block extra info support",
			ErrorCodes::LOGICAL_ERROR);
	return *block_extra_info;
}

void MultiplexedConnections::disconnect()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	for (auto it = replica_map.begin(); it != replica_map.end(); ++it)
	{
		ReplicaState & state = it->second;
		Connection * connection = connections[state.connection_index];
		if (connection != nullptr)
		{
			connection->disconnect();
			invalidateReplica(it);
		}
	}
}

void MultiplexedConnections::sendCancel()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (!sent_query || cancelled)
		throw Exception("Cannot cancel. Either no query sent or already cancelled.", ErrorCodes::LOGICAL_ERROR);

	for (const auto & e : replica_map)
	{
		const ReplicaState & state = e.second;
		Connection * connection = connections[state.connection_index];
		if (connection != nullptr)
			connection->sendCancel();
	}

	cancelled = true;
}

Connection::Packet MultiplexedConnections::drain()
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);

	if (!cancelled)
		throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

	Connection::Packet res;
	res.type = Protocol::Server::EndOfStream;

	while (hasActiveConnections())
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

std::string MultiplexedConnections::dumpAddresses() const
{
	Poco::ScopedLock<Poco::FastMutex> lock(cancel_mutex);
	return dumpAddressesUnlocked();
}

std::string MultiplexedConnections::dumpAddressesUnlocked() const
{
	bool is_first = true;
	std::ostringstream os;
	for (const auto & e : replica_map)
	{
		const ReplicaState & state = e.second;
		const Connection * connection = connections[state.connection_index];
		if (connection != nullptr)
		{
			os << (is_first ? "" : "; ") << connection->getDescription();
			is_first = false;
		}
	}

	return os.str();
}

void MultiplexedConnections::initFromShard(IConnectionPool * pool)
{
	auto entries = pool->getMany(settings, do_broadcast);

	/// Если getMany() не выделил соединений и не кинул исключения, это значит, что была
	/// установлена настройка skip_unavailable_shards. Тогда просто возвращаемся.
	if (entries.empty())
		return;

	ShardState shard_state;
	shard_state.allocated_connection_count = entries.size();
	shard_state.active_connection_count = entries.size();
	active_connection_total_count += shard_state.active_connection_count;

	shard_states.push_back(shard_state);

	pool_entries.insert(pool_entries.end(), entries.begin(), entries.end());
}

void MultiplexedConnections::registerShards()
{
	replica_map.reserve(pool_entries.size());
	connections.reserve(pool_entries.size());

	size_t offset = 0;
	for (auto & shard_state : shard_states)
	{
		size_t index_begin = offset;
		size_t index_end = offset + shard_state.allocated_connection_count;
		registerReplicas(index_begin, index_end, shard_state);
		offset = index_end;
	}
}

void MultiplexedConnections::registerReplicas(size_t index_begin, size_t index_end, ShardState & shard_state)
{
	for (size_t i = index_begin; i < index_end; ++i)
	{
		ReplicaState replica_state;
		replica_state.connection_index = i;
		replica_state.shard_state = &shard_state;

		Connection * connection = &*(pool_entries[i]);
		if (connection == nullptr)
			throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

		connection->setThrottler(throttler);
		connections.push_back(connection);

		auto res = replica_map.emplace(connection->socket.impl()->sockfd(), replica_state);
		if (!res.second)
			throw Exception("Invalid set of connections", ErrorCodes::LOGICAL_ERROR);
	}
}

Connection::Packet MultiplexedConnections::receivePacketUnlocked()
{
	if (!sent_query)
		throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
	if (!hasActiveConnections())
		throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

	auto it = getReplicaForReading();
	if (it == replica_map.end())
		throw Exception("Logical error: no available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

	ReplicaState & state = it->second;
	current_connection = connections[state.connection_index];
	if (current_connection == nullptr)
		throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

	Connection::Packet packet = current_connection->receivePacket();

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
			current_connection->disconnect();
			invalidateReplica(it);
			break;
	}

	return packet;
}

MultiplexedConnections::ReplicaMap::iterator MultiplexedConnections::getReplicaForReading()
{
	ReplicaMap::iterator it;

	if (supports_parallel_execution)
		it = waitForReadEvent();
	else
	{
		it = replica_map.begin();
		const ReplicaState & state = it->second;
		Connection * connection = connections[state.connection_index];
		if (connection == nullptr)
			it = replica_map.end();
	}

	return it;
}

MultiplexedConnections::ReplicaMap::iterator MultiplexedConnections::waitForReadEvent()
{
	Poco::Net::Socket::SocketList read_list;
	read_list.reserve(active_connection_total_count);

	/// Сначала проверяем, есть ли данные, которые уже лежат в буфере
	/// хоть одного соединения.
	for (const auto & e : replica_map)
	{
		const ReplicaState & state = e.second;
		Connection * connection = connections[state.connection_index];
		if ((connection != nullptr) && connection->hasReadBufferPendingData())
			read_list.push_back(connection->socket);
	}

	/// Если не было найдено никаких данных, то проверяем, есть ли соединения
	/// готовые для чтения.
	if (read_list.empty())
	{
		Poco::Net::Socket::SocketList write_list;
		Poco::Net::Socket::SocketList except_list;

		for (const auto & e : replica_map)
		{
			const ReplicaState & state = e.second;
			Connection * connection = connections[state.connection_index];
			if (connection != nullptr)
				read_list.push_back(connection->socket);
		}

		int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings->receive_timeout);

		if (n == 0)
			throw Exception("Timeout exceeded while reading from " + dumpAddressesUnlocked(), ErrorCodes::TIMEOUT_EXCEEDED);
	}

	auto & socket = read_list[rand() % read_list.size()];
	return replica_map.find(socket.impl()->sockfd());
}

void MultiplexedConnections::invalidateReplica(MultiplexedConnections::ReplicaMap::iterator it)
{
	ReplicaState & state = it->second;
	ShardState * shard_state = state.shard_state;

	connections[state.connection_index] = nullptr;
	--shard_state->active_connection_count;
	--active_connection_total_count;
}

}
