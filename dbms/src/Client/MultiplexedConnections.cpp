#include <Client/MultiplexedConnections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int NO_AVAILABLE_REPLICA;
    extern const int TIMEOUT_EXCEEDED;
}


MultiplexedConnections::MultiplexedConnections(Connection & connection, const Settings & settings_, const ThrottlerPtr & throttler_)
    : settings(settings_), throttler(throttler_), supports_parallel_execution(false)
{
    active_connection_total_count = 1;

    ShardState shard_state;
    shard_state.allocated_connection_count = active_connection_total_count;
    shard_state.active_connection_count = active_connection_total_count;

    shard_states.push_back(shard_state);

    ReplicaState replica_state;
    replica_state.connection_index = 0;
    replica_state.shard_state = &shard_states[0];

    connection.setThrottler(throttler);
    connections.push_back(&connection);

    replica_map.emplace(connections[0]->socket.impl()->sockfd(), replica_state);
}

MultiplexedConnections::MultiplexedConnections(
        std::vector<IConnectionPool::Entry> && connections,
        const Settings & settings_, const ThrottlerPtr & throttler_, bool append_extra_info)
    : settings(settings_), throttler(throttler_)
{
    initShard(connections);
    registerShards();

    supports_parallel_execution = active_connection_total_count > 1;

    if (append_extra_info)
        block_extra_info = std::make_unique<BlockExtraInfo>();
}

MultiplexedConnections::MultiplexedConnections(
        ConnectionPoolWithFailover & pool,
        const Settings & settings_, const ThrottlerPtr & throttler_, bool append_extra_info,
        PoolMode pool_mode, const QualifiedTableName * main_table)
    : settings(settings_), throttler(throttler_)
{
    initShard(pool, pool_mode, main_table);
    registerShards();

    supports_parallel_execution = active_connection_total_count > 1;

    if (append_extra_info)
        block_extra_info = std::make_unique<BlockExtraInfo>();
}

MultiplexedConnections::MultiplexedConnections(
        const ConnectionPoolWithFailoverPtrs & pools, const Settings & settings_, const ThrottlerPtr & throttler_,
        bool append_extra_info, PoolMode pool_mode, const QualifiedTableName * main_table)
    : settings(settings_), throttler(throttler_)
{
    if (pools.empty())
        throw Exception("Pools are not specified", ErrorCodes::LOGICAL_ERROR);

    for (auto & pool : pools)
    {
        if (!pool)
            throw Exception("Invalid pool specified", ErrorCodes::LOGICAL_ERROR);
        initShard(*pool, pool_mode, main_table);
    }

    registerShards();

    supports_parallel_execution = active_connection_total_count > 1;

    if (append_extra_info)
        block_extra_info = std::make_unique<BlockExtraInfo>();
}

void MultiplexedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard<std::mutex> lock(cancel_mutex);

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

void MultiplexedConnections::sendQuery(
    const String & query,
    const String & query_id,
    UInt64 stage,
    const ClientInfo * client_info,
    bool with_pending_data)
{
    std::lock_guard<std::mutex> lock(cancel_mutex);

    if (sent_query)
        throw Exception("Query already sent.", ErrorCodes::LOGICAL_ERROR);

    if (supports_parallel_execution)
    {
        /// Each shard has one or more replicas.
        auto it = connections.begin();
        for (const auto & shard_state : shard_states)
        {
            Settings query_settings = settings;
            query_settings.parallel_replicas_count = shard_state.active_connection_count;

            UInt64 offset = 0;

            for (size_t i = 0; i < shard_state.allocated_connection_count; ++i)
            {
                Connection * connection = *it;
                if (connection == nullptr)
                    throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

                query_settings.parallel_replica_offset = offset;
                connection->sendQuery(query, query_id, stage, &query_settings, client_info, with_pending_data);
                ++offset;
                ++it;
            }
        }
    }
    else
    {
        Connection * connection = connections[0];
        if (connection == nullptr)
            throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

        connection->sendQuery(query, query_id, stage, &settings, client_info, with_pending_data);
    }

    sent_query = true;
}

Connection::Packet MultiplexedConnections::receivePacket()
{
    std::lock_guard<std::mutex> lock(cancel_mutex);
    Connection::Packet packet = receivePacketUnlocked();
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
    std::lock_guard<std::mutex> lock(cancel_mutex);

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
    std::lock_guard<std::mutex> lock(cancel_mutex);

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
    std::lock_guard<std::mutex> lock(cancel_mutex);

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
                /// If we receive an exception or an unknown packet, we save it.
                res = std::move(packet);
                break;
        }
    }

    return res;
}

std::string MultiplexedConnections::dumpAddresses() const
{
    std::lock_guard<std::mutex> lock(cancel_mutex);
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

void MultiplexedConnections::initShard(ConnectionPoolWithFailover & pool, PoolMode pool_mode, const QualifiedTableName * main_table)
{
    std::vector<IConnectionPool::Entry> connections;
    if (main_table)
    {
        auto try_results = pool.getManyChecked(&settings, pool_mode, *main_table);
        connections.reserve(try_results.size());
        for (auto & try_result : try_results)
            connections.emplace_back(std::move(try_result.entry));
    }
    else
        connections = pool.getMany(&settings, pool_mode);

    initShard(connections);
}

void MultiplexedConnections::initShard(const std::vector<IConnectionPool::Entry> & connections)
{
    /// If getMany() did not allocate connections and did not throw exceptions, this means that
    /// `skip_unavailable_shards` was set. Then just return.
    if (connections.empty())
        return;

    ShardState shard_state;
    shard_state.allocated_connection_count = connections.size();
    shard_state.active_connection_count = connections.size();
    active_connection_total_count += shard_state.active_connection_count;

    shard_states.push_back(shard_state);

    pool_entries.insert(pool_entries.end(), connections.begin(), connections.end());
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

    /// First, we check if there are data already in the buffer
    /// of at least one connection.
    for (const auto & e : replica_map)
    {
        const ReplicaState & state = e.second;
        Connection * connection = connections[state.connection_index];
        if ((connection != nullptr) && connection->hasReadBufferPendingData())
            read_list.push_back(connection->socket);
    }

    /// If no data was found, then we check if there are any connections
    /// ready for reading.
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

        int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings.receive_timeout);

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
