#include <Client/MultiplexedConnections.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Operators.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int NO_AVAILABLE_REPLICA;
    extern const int TIMEOUT_EXCEEDED;
}


MultiplexedConnections::MultiplexedConnections(Connection & connection, const Settings & settings_, const ThrottlerPtr & throttler)
    : settings(settings_)
{
    connection.setThrottler(throttler);

    ReplicaState replica_state;
    replica_state.connection = &connection;
    replica_states.push_back(replica_state);

    active_connection_count = 1;
}

MultiplexedConnections::MultiplexedConnections(
        std::vector<IConnectionPool::Entry> && connections,
        const Settings & settings_, const ThrottlerPtr & throttler)
    : settings(settings_)
{
    /// If we didn't get any connections from pool and getMany() did not throw exceptions, this means that
    /// `skip_unavailable_shards` was set. Then just return.
    if (connections.empty())
        return;

    replica_states.reserve(connections.size());
    for (auto & connection : connections)
    {
        connection->setThrottler(throttler);

        ReplicaState replica_state;
        replica_state.connection = &*connection;
        replica_state.pool_entry = std::move(connection);

        replica_states.push_back(std::move(replica_state));
    }

    active_connection_count = connections.size();
}

void MultiplexedConnections::sendScalarsData(Scalars & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send scalars data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
            connection->sendScalarsData(data);
    }
}

void MultiplexedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    if (data.size() != active_connection_count)
        throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

    auto it = data.begin();
    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
        {
            connection->sendExternalTablesData(*it);
            ++it;
        }
    }
}

void MultiplexedConnections::sendQuery(
    const ConnectionTimeouts & timeouts,
    const String & query,
    const String & query_id,
    UInt64 stage,
    const ClientInfo & client_info,
    bool with_pending_data)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception("Query already sent.", ErrorCodes::LOGICAL_ERROR);

    Settings modified_settings = settings;

    for (auto & replica : replica_states)
    {
        if (!replica.connection)
            throw Exception("MultiplexedConnections: Internal error", ErrorCodes::LOGICAL_ERROR);

        if (replica.connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
        {
            /// Disable two-level aggregation due to version incompatibility.
            modified_settings.group_by_two_level_threshold = 0;
            modified_settings.group_by_two_level_threshold_bytes = 0;
        }
    }

    size_t num_replicas = replica_states.size();
    if (num_replicas > 1)
    {
        /// Use multiple replicas for parallel query processing.
        modified_settings.parallel_replicas_count = num_replicas;
        for (size_t i = 0; i < num_replicas; ++i)
        {
            modified_settings.parallel_replica_offset = i;
            replica_states[i].connection->sendQuery(timeouts, query, query_id,
                stage, &modified_settings, &client_info, with_pending_data);
        }
    }
    else
    {
        /// Use single replica.
        replica_states[0].connection->sendQuery(timeouts, query, query_id,
                stage, &modified_settings, &client_info, with_pending_data);
    }

    sent_query = true;
}

void MultiplexedConnections::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception("Cannot send uuids after query is sent.", ErrorCodes::LOGICAL_ERROR);

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
            connection->sendIgnoredPartUUIDs(uuids);
    }
}

Packet MultiplexedConnections::receivePacket()
{
    std::lock_guard lock(cancel_mutex);
    Packet packet = receivePacketUnlocked();
    return packet;
}

void MultiplexedConnections::disconnect()
{
    std::lock_guard lock(cancel_mutex);

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
        {
            connection->disconnect();
            invalidateReplica(state);
        }
    }
}

void MultiplexedConnections::sendCancel()
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query || cancelled)
        throw Exception("Cannot cancel. Either no query sent or already cancelled.", ErrorCodes::LOGICAL_ERROR);

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
            connection->sendCancel();
    }

    cancelled = true;
}

Packet MultiplexedConnections::drain()
{
    std::lock_guard lock(cancel_mutex);

    if (!cancelled)
        throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

    Packet res;
    res.type = Protocol::Server::EndOfStream;

    while (hasActiveConnections())
    {
        Packet packet = receivePacketUnlocked();

        switch (packet.type)
        {
            case Protocol::Server::PartUUIDs:
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
    std::lock_guard lock(cancel_mutex);
    return dumpAddressesUnlocked();
}

std::string MultiplexedConnections::dumpAddressesUnlocked() const
{
    bool is_first = true;
    WriteBufferFromOwnString buf;
    for (const ReplicaState & state : replica_states)
    {
        const Connection * connection = state.connection;
        if (connection)
        {
            buf << (is_first ? "" : "; ") << connection->getDescription();
            is_first = false;
        }
    }

    return buf.str();
}

Packet MultiplexedConnections::receivePacketUnlocked(std::function<void(Poco::Net::Socket &)> async_callback)
{
    if (!sent_query)
        throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
    if (!hasActiveConnections())
        throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

    ReplicaState & state = getReplicaForReading();
    current_connection = state.connection;
    if (current_connection == nullptr)
        throw Exception("Logical error: no available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

    Packet packet = current_connection->receivePacket(std::move(async_callback));

    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
        case Protocol::Server::Data:
        case Protocol::Server::Progress:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
            break;

        case Protocol::Server::EndOfStream:
            invalidateReplica(state);
            break;

        case Protocol::Server::Exception:
        default:
            current_connection->disconnect();
            invalidateReplica(state);
            break;
    }

    return packet;
}

MultiplexedConnections::ReplicaState & MultiplexedConnections::getReplicaForReading()
{
    if (replica_states.size() == 1)
        return replica_states[0];

    Poco::Net::Socket::SocketList read_list;
    read_list.reserve(active_connection_count);

    /// First, we check if there are data already in the buffer
    /// of at least one connection.
    for (const ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if ((connection != nullptr) && connection->hasReadPendingData())
            read_list.push_back(*connection->socket);
    }

    /// If no data was found, then we check if there are any connections ready for reading.
    if (read_list.empty())
    {
        Poco::Net::Socket::SocketList write_list;
        Poco::Net::Socket::SocketList except_list;

        for (const ReplicaState & state : replica_states)
        {
            Connection * connection = state.connection;
            if (connection != nullptr)
                read_list.push_back(*connection->socket);
        }

        int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings.receive_timeout);

        if (n == 0)
            throw Exception("Timeout exceeded while reading from " + dumpAddressesUnlocked(), ErrorCodes::TIMEOUT_EXCEEDED);
    }

    /// TODO Absolutely wrong code: read_list could be empty; motivation of rand is unclear.
    /// This code path is disabled by default.

    auto & socket = read_list[thread_local_rng() % read_list.size()];
    if (fd_to_replica_state_idx.empty())
    {
        fd_to_replica_state_idx.reserve(replica_states.size());
        size_t replica_state_number = 0;
        for (const auto & replica_state : replica_states)
        {
            fd_to_replica_state_idx.emplace(replica_state.connection->socket->impl()->sockfd(), replica_state_number);
            ++replica_state_number;
        }
    }
    return replica_states[fd_to_replica_state_idx.at(socket.impl()->sockfd())];
}

void MultiplexedConnections::invalidateReplica(ReplicaState & state)
{
    state.connection = nullptr;
    state.pool_entry = IConnectionPool::Entry();
    --active_connection_count;
}

}
