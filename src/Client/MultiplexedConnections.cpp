#include <Client/MultiplexedConnections.h>

#include <Common/thread_local_rng.h>
#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Operators.h>
#include <Interpreters/ClientInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int NO_AVAILABLE_REPLICA;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}


MultiplexedConnections::MultiplexedConnections(Connection & connection, const Settings & settings_, const ThrottlerPtr & throttler)
    : settings(settings_), drain_timeout(settings.drain_timeout), receive_timeout(settings.receive_timeout)
{
    connection.setThrottler(throttler);

    ReplicaState replica_state;
    replica_state.connection = &connection;
    replica_states.push_back(replica_state);

    active_connection_count = 1;
}


MultiplexedConnections::MultiplexedConnections(std::shared_ptr<Connection> connection_ptr_, const Settings & settings_, const ThrottlerPtr & throttler)
    : settings(settings_), drain_timeout(settings.drain_timeout), receive_timeout(settings.receive_timeout)
    , connection_ptr(connection_ptr_)
{
    connection_ptr->setThrottler(throttler);

    ReplicaState replica_state;
    replica_state.connection = connection_ptr.get();
    replica_states.push_back(replica_state);

    active_connection_count = 1;
}

MultiplexedConnections::MultiplexedConnections(
    std::vector<IConnectionPool::Entry> && connections, const Settings & settings_, const ThrottlerPtr & throttler)
    : settings(settings_), drain_timeout(settings.drain_timeout), receive_timeout(settings.receive_timeout)
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
    ClientInfo & client_info,
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

        bool parallel_reading_from_replicas = settings.max_parallel_replicas > 1
            && settings.allow_experimental_parallel_reading_from_replicas
            /// To avoid trying to coordinate with clickhouse-benchmark,
            /// since it uses the same code.
            && client_info.query_kind != ClientInfo::QueryKind::INITIAL_QUERY;
        if (parallel_reading_from_replicas)
        {
            client_info.collaborate_with_initiator = true;
            client_info.count_participating_replicas = replica_info.all_replicas_count;
            client_info.number_of_current_replica = replica_info.number_of_current_replica;
        }
    }

    const bool enable_sample_offset_parallel_processing = settings.max_parallel_replicas > 1 && !settings.allow_experimental_parallel_reading_from_replicas;

    size_t num_replicas = replica_states.size();
    if (num_replicas > 1)
    {
        if (enable_sample_offset_parallel_processing)
            /// Use multiple replicas for parallel query processing.
            modified_settings.parallel_replicas_count = num_replicas;

        for (size_t i = 0; i < num_replicas; ++i)
        {
            if (enable_sample_offset_parallel_processing)
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


void MultiplexedConnections::sendReadTaskResponse(const String & response)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    current_connection->sendReadTaskResponse(response);
}


void MultiplexedConnections::sendMergeTreeReadTaskResponse(PartitionReadResponse response)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    current_connection->sendMergeTreeReadTaskResponse(response);
}


Packet MultiplexedConnections::receivePacket()
{
    std::lock_guard lock(cancel_mutex);
    Packet packet = receivePacketUnlocked({}, false /* is_draining */);
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
        Packet packet = receivePacketUnlocked(DrainCallback{drain_timeout}, true /* is_draining */);

        switch (packet.type)
        {
            case Protocol::Server::MergeTreeReadTaskRequest:
            case Protocol::Server::ReadTaskRequest:
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

Packet MultiplexedConnections::receivePacketUnlocked(AsyncCallback async_callback, bool is_draining)
{
    if (!sent_query)
        throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
    if (!hasActiveConnections())
        throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

    ReplicaState & state = getReplicaForReading(is_draining);
    current_connection = state.connection;
    if (current_connection == nullptr)
        throw Exception("Logical error: no available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

    Packet packet;
    {
        AsyncCallbackSetter async_setter(current_connection, std::move(async_callback));

        try
        {
            packet = current_connection->receivePacket();
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_SERVER)
            {
                /// Exception may happen when packet is received, e.g. when got unknown packet.
                /// In this case, invalidate replica, so that we would not read from it anymore.
                current_connection->disconnect();
                invalidateReplica(state);
            }
            throw;
        }
    }

    switch (packet.type)
    {
        case Protocol::Server::MergeTreeReadTaskRequest:
        case Protocol::Server::ReadTaskRequest:
        case Protocol::Server::PartUUIDs:
        case Protocol::Server::Data:
        case Protocol::Server::Progress:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
        case Protocol::Server::ProfileEvents:
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

MultiplexedConnections::ReplicaState & MultiplexedConnections::getReplicaForReading(bool is_draining)
{
    /// Fast path when we only focus on one replica and are not draining the connection.
    if (replica_states.size() == 1 && !is_draining)
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

        auto timeout = is_draining ? drain_timeout : receive_timeout;
        int n = Poco::Net::Socket::select(
            read_list,
            write_list,
            except_list,
            timeout);

        /// We treat any error as timeout for simplicity.
        /// And we also check if read_list is still empty just in case.
        if (n <= 0 || read_list.empty())
        {
            for (ReplicaState & state : replica_states)
            {
                Connection * connection = state.connection;
                if (connection != nullptr)
                {
                    connection->disconnect();
                    invalidateReplica(state);
                }
            }
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout ({} ms) exceeded while reading from {}",
                timeout.totalMilliseconds(),
                dumpAddressesUnlocked());
        }
    }

    /// TODO Motivation of rand is unclear.
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
