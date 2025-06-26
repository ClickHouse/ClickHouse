#include <Client/MultiplexedConnections.h>

#include <Common/thread_local_rng.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Operators.h>
#include <Interpreters/ClientInfo.h>
#include <base/getThreadId.h>
#include <base/hex.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsDialect dialect;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsUInt64 parallel_replica_offset;
    extern const SettingsSeconds receive_timeout;
}

// NOLINTBEGIN(bugprone-undefined-memory-manipulation)

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int NO_AVAILABLE_REPLICA;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}


MultiplexedConnections::MultiplexedConnections(Connection & connection, ContextPtr context_, const ThrottlerPtr & throttler)
    : context(std::move(context_)), settings(context->getSettingsRef())
{
    connection.setThrottler(throttler);

    ReplicaState replica_state;
    replica_state.connection = &connection;
    replica_states.push_back(replica_state);

    active_connection_count = 1;
}


MultiplexedConnections::MultiplexedConnections(
    std::shared_ptr<Connection> connection_ptr_, ContextPtr context_, const ThrottlerPtr & throttler)
    : context(std::move(context_)), settings(context->getSettingsRef()), connection_ptr(connection_ptr_)
{
    connection_ptr->setThrottler(throttler);

    ReplicaState replica_state;
    replica_state.connection = connection_ptr.get();
    replica_states.push_back(replica_state);

    active_connection_count = 1;
}

MultiplexedConnections::MultiplexedConnections(
    std::vector<IConnectionPool::Entry> && connections, ContextPtr context_, const ThrottlerPtr & throttler)
    : context(std::move(context_)), settings(context->getSettingsRef())
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send scalars data: query not yet sent.");

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
            connection->sendScalarsData(data);
    }
}

void MultiplexedConnections::sendQueryPlan(const QueryPlan & query_plan)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send scalars data: query not yet sent.");

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
            connection->sendQueryPlan(query_plan);
    }
}

void MultiplexedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send external tables data: query not yet sent.");

    if (data.size() != active_connection_count)
        throw Exception(ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES, "Mismatch between replicas and data sources");

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
    bool with_pending_data,
    const std::vector<String> & external_roles)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query already sent.");

    Settings modified_settings = settings;

    /// Queries in foreign languages are transformed to ClickHouse-SQL. Ensure the setting before sending.
    modified_settings[Setting::dialect] = Dialect::clickhouse;
    modified_settings[Setting::dialect].changed = false;

    for (auto & replica : replica_states)
    {
        if (!replica.connection)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiplexedConnections: Internal error");

        if (replica.connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
        {
            /// Disable two-level aggregation due to version incompatibility.
            modified_settings[Setting::group_by_two_level_threshold] = 0;
            modified_settings[Setting::group_by_two_level_threshold_bytes] = 0;
        }
    }

    if (replica_info)
    {
        client_info.collaborate_with_initiator = true;
        client_info.number_of_current_replica = replica_info->number_of_current_replica;
    }

    /// FIXME: Remove once we will make `allow_experimental_analyzer` obsolete setting.
    /// Make the analyzer being set, so it will be effectively applied on the remote server.
    /// In other words, the initiator always controls whether the analyzer enabled or not for
    /// all servers involved in the distributed query processing.
    modified_settings.set("allow_experimental_analyzer", static_cast<bool>(modified_settings[Setting::allow_experimental_analyzer]));

    const bool enable_offset_parallel_processing = context->canUseOffsetParallelReplicas();

    size_t num_replicas = replica_states.size();
    if (num_replicas > 1)
    {
        if (enable_offset_parallel_processing)
            /// Use multiple replicas for parallel query processing.
            modified_settings[Setting::parallel_replicas_count] = num_replicas;

        for (size_t i = 0; i < num_replicas; ++i)
        {
            if (enable_offset_parallel_processing)
                modified_settings[Setting::parallel_replica_offset] = i;

            replica_states[i].connection->sendQuery(
                timeouts, query, /* query_parameters */ {}, query_id, stage, &modified_settings, &client_info, with_pending_data, external_roles, {});
        }
    }
    else
    {
        /// Use single replica.
        replica_states[0].connection->sendQuery(
            timeouts, query, /* query_parameters */ {}, query_id, stage, &modified_settings, &client_info, with_pending_data, external_roles, {});
    }

    sent_query = true;
}


void MultiplexedConnections::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send uuids after query is sent.");

    for (ReplicaState & state : replica_states)
    {
        Connection * connection = state.connection;
        if (connection != nullptr)
            connection->sendIgnoredPartUUIDs(uuids);
    }
}


void MultiplexedConnections::sendClusterFunctionReadTaskResponse(const ClusterFunctionReadTaskResponse & response)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    current_connection->sendClusterFunctionReadTaskResponse(response);
}


void MultiplexedConnections::sendMergeTreeReadTaskResponse(const ParallelReadResponse & response)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    current_connection->sendMergeTreeReadTaskResponse(response);
}


Packet MultiplexedConnections::receivePacket()
{
    std::lock_guard lock(cancel_mutex);
    Packet packet = receivePacketUnlocked({});
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot cancel. Either no query sent or already cancelled.");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot drain connections: cancel first.");

    Packet res;
    res.type = Protocol::Server::EndOfStream;

    while (hasActiveConnections())
    {
        Packet packet = receivePacketUnlocked({});

        switch (packet.type)
        {
            case Protocol::Server::TimezoneUpdate:
            case Protocol::Server::MergeTreeAllRangesAnnouncement:
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

UInt64 MultiplexedConnections::receivePacketTypeUnlocked(AsyncCallback async_callback)
{
    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot receive packets: no query sent.");
    if (!hasActiveConnections())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No more packets are available.");

    ReplicaState & state = getReplicaForReading();
    current_connection = state.connection;
    if (current_connection == nullptr)
        throw Exception(ErrorCodes::NO_AVAILABLE_REPLICA, "No available replica");

    try
    {
        AsyncCallbackSetter async_setter(current_connection, std::move(async_callback));
        return current_connection->receivePacketType();
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

Packet MultiplexedConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot receive packets: no query sent.");
    if (!hasActiveConnections())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No more packets are available.");

    ReplicaState & state = getReplicaForReading();
    current_connection = state.connection;
    if (current_connection == nullptr)
        throw Exception(ErrorCodes::NO_AVAILABLE_REPLICA, "No available replica");

    Packet packet;
    try
    {
        AsyncCallbackSetter async_setter(current_connection, std::move(async_callback));
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

    switch (packet.type)
    {
        case Protocol::Server::TimezoneUpdate:
        case Protocol::Server::MergeTreeAllRangesAnnouncement:
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

        auto timeout = settings[Setting::receive_timeout];
        int n = 0;

        /// EINTR loop
        while (true)
        {
            read_list.clear();
            for (const ReplicaState & state : replica_states)
            {
                Connection * connection = state.connection;
                if (connection != nullptr)
                    read_list.push_back(*connection->socket);
            }

            /// poco returns 0 on EINTR, let's reset errno to ensure that EINTR came from select().
            errno = 0;

            n = Poco::Net::Socket::select(
                read_list,
                write_list,
                except_list,
                timeout);
            if (n <= 0 && errno == EINTR)
                continue;
            break;
        }

        if (n == 0)
        {
            const auto & addresses = dumpAddressesUnlocked();
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
                addresses);
        }
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

void MultiplexedConnections::setAsyncCallback(AsyncCallback async_callback)
{
    for (ReplicaState & state : replica_states)
    {
        if (state.connection)
            state.connection->setAsyncCallback(async_callback);
    }
}

// NOLINTEND(bugprone-undefined-memory-manipulation)

}
