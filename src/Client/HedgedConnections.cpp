#if defined(OS_LINUX)

#include <Client/HedgedConnections.h>
#include <Interpreters/ClientInfo.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int LOGICAL_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

HedgedConnections::HedgedConnections(
    const ConnectionPoolWithFailoverPtr & pool_,
    const Settings & settings_,
    const ConnectionTimeouts & timeouts_,
    const ThrottlerPtr & throttler_,
    PoolMode pool_mode,
    std::shared_ptr<QualifiedTableName> table_to_check_)
    : hedged_connections_factory(pool_, &settings_, timeouts_, table_to_check_)
    , settings(settings_)
    , throttler(throttler_)
{
    std::vector<Connection *> connections = hedged_connections_factory.getManyConnections(pool_mode);

    if (connections.empty())
        return;

    for (size_t i = 0; i != connections.size(); ++i)
    {
        ReplicaState replica;
        replica.connection = connections[i];
        replica.connection->setThrottler(throttler_);
        replica.epoll.add(replica.connection->getSocket()->impl()->sockfd());
        epoll.add(replica.epoll.getFileDescriptor());
        fd_to_replica_location[replica.epoll.getFileDescriptor()] = ReplicaLocation{i, 0};
        offset_states.emplace_back();
        offset_states[i].replicas.emplace_back(std::move(replica));
        offset_states[i].active_connection_count = 1;
    }

    active_connection_count = connections.size();
    offsets_with_received_first_data_packet = 0;
    pipeline_for_new_replicas.add([throttler_](ReplicaState & replica_) { replica_.connection->setThrottler(throttler_); });

    log = &Poco::Logger::get("HedgedConnections");
}

void HedgedConnections::Pipeline::add(std::function<void(ReplicaState & replica)> send_function)
{
    pipeline.push_back(send_function);
}

void HedgedConnections::Pipeline::run(ReplicaState & replica)
{
    for (auto & send_func : pipeline)
        send_func(replica);
}

void HedgedConnections::sendScalarsData(Scalars & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send scalars data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    auto send_scalars_data = [&data](ReplicaState & replica) { replica.connection->sendScalarsData(data); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_scalars_data(replica);

    pipeline_for_new_replicas.add(send_scalars_data);
}

void HedgedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    if (data.size() != size())
        throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

    auto send_external_tables_data = [&data](ReplicaState & replica) { replica.connection->sendExternalTablesData(data[0]); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_external_tables_data(replica);

    pipeline_for_new_replicas.add(send_external_tables_data);
}

void HedgedConnections::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception("Cannot send uuids after query is sent.", ErrorCodes::LOGICAL_ERROR);

    auto send_ignored_part_uuids = [&uuids](ReplicaState & replica) { replica.connection->sendIgnoredPartUUIDs(uuids); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_ignored_part_uuids(replica);

    pipeline_for_new_replicas.add(send_ignored_part_uuids);
}

void HedgedConnections::sendQuery(
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

    for (auto & offset_state : offset_states)
    {
        for (auto & replica : offset_state.replicas)
        {
            if (replica.connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
            {
                disable_two_level_aggregation = true;
                break;
            }
        }
        if (disable_two_level_aggregation)
            break;
    }

    auto send_query = [this, timeouts, query, query_id, stage, client_info, with_pending_data](ReplicaState & replica)
    {
        Settings modified_settings = settings;

        if (disable_two_level_aggregation)
        {
            /// Disable two-level aggregation due to version incompatibility.
            modified_settings.group_by_two_level_threshold = 0;
            modified_settings.group_by_two_level_threshold_bytes = 0;
        }

        if (offset_states.size() > 1)
        {
            modified_settings.parallel_replicas_count = offset_states.size();
            modified_settings.parallel_replica_offset = fd_to_replica_location[replica.epoll.getFileDescriptor()].offset;
        }

        replica.connection->sendQuery(timeouts, query, query_id, stage, &modified_settings, &client_info, with_pending_data);
        replica.receive_timeout.setRelative(timeouts.receive_timeout);
        replica.change_replica_timeout.setRelative(timeouts.receive_data_timeout);
    };

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            send_query(replica);

    pipeline_for_new_replicas.add(send_query);
    sent_query = true;
}

void HedgedConnections::disconnect()
{
    std::lock_guard lock(cancel_mutex);

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            if (replica.connection)
                finishProcessReplica(replica, true);

    if (hedged_connections_factory.hasEventsInProcess())
    {
        if (next_replica_in_process)
        {
            epoll.remove(hedged_connections_factory.getFileDescriptor());
            next_replica_in_process = false;
        }

        hedged_connections_factory.stopChoosingReplicas();
    }
}

std::string HedgedConnections::dumpAddresses() const
{
    std::lock_guard lock(cancel_mutex);

    std::string addresses;
    bool is_first = true;

    for (const auto & offset_state : offset_states)
    {
        for (const auto & replica : offset_state.replicas)
        {
            if (replica.connection)
            {
                addresses += (is_first ? "" : "; ") + replica.connection->getDescription();
                is_first = false;
            }
        }
    }

    return addresses;
}

void HedgedConnections::sendCancel()
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query || cancelled)
        throw Exception("Cannot cancel. Either no query sent or already cancelled.", ErrorCodes::LOGICAL_ERROR);

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            if (replica.connection)
                replica.connection->sendCancel();

    cancelled = true;
}

Packet HedgedConnections::drain()
{
    std::lock_guard lock(cancel_mutex);

    if (!cancelled)
        throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

    Packet res;
    res.type = Protocol::Server::EndOfStream;

    while (!epoll.empty())
    {
        ReplicaLocation location = getReadyReplicaLocation();
        Packet packet = receivePacketFromReplica(location);
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

Packet HedgedConnections::receivePacket()
{
    std::lock_guard lock(cancel_mutex);
    return receivePacketUnlocked({});
}

Packet HedgedConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    if (!sent_query)
        throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
    if (!hasActiveConnections())
        throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

    if (epoll.empty())
        throw Exception("No pending events in epoll.", ErrorCodes::LOGICAL_ERROR);

    ReplicaLocation location = getReadyReplicaLocation(async_callback);
    return receivePacketFromReplica(location, std::move(async_callback));
}

HedgedConnections::ReplicaLocation HedgedConnections::getReadyReplicaLocation(AsyncCallback async_callback)
{
    LOG_DEBUG(log, "getReadyReplicaLocation");
    int event_fd;
    while (true)
    {
        /// Check connections for pending data.
        ReplicaLocation location;
        if (checkPendingData(location))
            return location;

        /// Get ready file descriptor from epoll and process it.
        event_fd = getReadyFileDescriptor(async_callback);

        if (event_fd == hedged_connections_factory.getFileDescriptor())
        {
            tryGetNewReplica(false);
            continue;
        }

        if (!fd_to_replica_location.contains(event_fd))
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);

        location = fd_to_replica_location[event_fd];

        /// Read all events from replica epoll.
        /// If socket is ready and timeout is alarmed simultaneously, skip timeout.
        bool is_socket_ready = false;
        bool is_change_replica_timeout_alarmed = false;
        bool is_receive_timeout_alarmed = false;

        epoll_event events[3];
        events[0].data.fd = events[1].data.fd = events[2].data.fd = -1;
        ReplicaState & replica_state = offset_states[location.offset].replicas[location.index];
        size_t ready_count = replica_state.epoll.getManyReady(3, events, true);

        for (size_t i = 0; i != ready_count; ++i)
        {
            if (events[i].data.fd == replica_state.connection->getSocket()->impl()->sockfd())
                is_socket_ready = true;
            if (events[i].data.fd == replica_state.change_replica_timeout.getDescriptor())
                is_change_replica_timeout_alarmed = true;
            if (events[i].data.fd == replica_state.receive_timeout.getDescriptor())
                is_receive_timeout_alarmed = true;
        }

        if (is_socket_ready)
            return location;

        /// We reach this point only if there is an alarmed timeout.

        if (is_change_replica_timeout_alarmed)
        {
            replica_state.change_replica_timeout.reset();
            offsets_queue.push(location.offset);
            tryGetNewReplica(true);
        }
        if (is_receive_timeout_alarmed)
        {
            finishProcessReplica(replica_state, true);

            /// Check if there is no more active connections with the same offset and there is no new replica in process.
            if (offset_states[location.offset].active_connection_count == 0 && !next_replica_in_process)
                throw NetException("Receive timeout expired", ErrorCodes::SOCKET_TIMEOUT);
        }
    }
};

int HedgedConnections::getReadyFileDescriptor(AsyncCallback async_callback)
{
    epoll_event event;
    event.data.fd = -1;
    epoll.getManyReady(1, &event, true, std::move(async_callback));
    return event.data.fd;
}

bool HedgedConnections::checkPendingData(ReplicaLocation & location_out)
{
    for (auto & [fd, location] : fd_to_replica_location)
    {
        if (offset_states[location.offset].replicas[location.index].connection->hasReadPendingData())
        {
            location_out = location;
            return true;
        }
    }

    return false;
}

Packet HedgedConnections::receivePacketFromReplica(const ReplicaLocation & replica_location, AsyncCallback async_callback)
{
    LOG_DEBUG(log, "receivePacketFromReplica");

    ReplicaState & replica = offset_states[replica_location.offset].replicas[replica_location.index];
    replica.receive_timeout.reset();
    Packet packet = replica.connection->receivePacket(std::move(async_callback));
    switch (packet.type)
    {
        case Protocol::Server::Data:
            if (!offset_states[replica_location.offset].first_packet_of_data_received)
                processReceivedFirstDataPacket(replica_location);
            replica.receive_timeout.setRelative(hedged_connections_factory.getConnectionTimeouts().receive_timeout);
            break;
        case Protocol::Server::PartUUIDs:
        case Protocol::Server::Progress:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
            replica.receive_timeout.setRelative(hedged_connections_factory.getConnectionTimeouts().receive_timeout);
            break;

        case Protocol::Server::EndOfStream:
            finishProcessReplica(replica, false);
            break;

        case Protocol::Server::Exception:
        default:
            finishProcessReplica(replica, true);
            break;
    }

    return packet;
}

void HedgedConnections::processReceivedFirstDataPacket(const ReplicaLocation & replica_location)
{
    LOG_DEBUG(log, "processReceivedFirstDataPacket");

    /// When we receive first packet of data from replica, we stop working with replicas, that are
    /// responsible for the same offset.
    OffsetState & offset_state = offset_states[replica_location.offset];
    offset_state.replicas[replica_location.index].change_replica_timeout.reset();
    ++offsets_with_received_first_data_packet;
    offset_state.first_packet_of_data_received = true;

    for (size_t i = 0; i != offset_state.replicas.size(); ++i)
    {
        if (i != replica_location.index && offset_state.replicas[i].connection)
        {
            offset_state.replicas[i].connection->sendCancel();
            finishProcessReplica(offset_state.replicas[i], true);
        }
    }

    /// If we received data from replicas with all offsets, we need to stop choosing new replicas.
    if (hedged_connections_factory.hasEventsInProcess() && offsets_with_received_first_data_packet == offset_states.size())
    {
        if (next_replica_in_process)
        {
            epoll.remove(hedged_connections_factory.getFileDescriptor());
            next_replica_in_process = false;
        }
        hedged_connections_factory.stopChoosingReplicas();
    }
}

void HedgedConnections::tryGetNewReplica(bool start_new_connection)
{
    LOG_DEBUG(log, "tryGetNewReplica");

    Connection * connection = nullptr;
    HedgedConnectionsFactory::State state = hedged_connections_factory.getNextConnection(start_new_connection, false, connection);

    /// Skip replicas that doesn't support two-level aggregation if we didn't disable it in sendQuery.
    while (state == HedgedConnectionsFactory::State::READY && !disable_two_level_aggregation
           && connection->getServerRevision(hedged_connections_factory.getConnectionTimeouts())
               < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
        state = hedged_connections_factory.getNextConnection(true, false, connection);

    if (state == HedgedConnectionsFactory::State::READY)
    {
        size_t offset = offsets_queue.front();
        offsets_queue.pop();

        ReplicaState replica;
        replica.connection = connection;
        replica.epoll.add(replica.connection->getSocket()->impl()->sockfd());
        epoll.add(replica.epoll.getFileDescriptor());
        fd_to_replica_location[replica.epoll.getFileDescriptor()] = ReplicaLocation{offset, offset_states[offset].replicas.size()};
        ++active_connection_count;
        pipeline_for_new_replicas.run(replica);
        offset_states[offset].replicas.push_back(std::move(replica));
    }
    else if (state == HedgedConnectionsFactory::State::NOT_READY && !next_replica_in_process)
    {
        epoll.add(hedged_connections_factory.getFileDescriptor());
        next_replica_in_process = true;
    }

    /// Check if we cannot get new replica and there is no active replica with needed offsets.
    else if (state == HedgedConnectionsFactory::State::CANNOT_CHOOSE)
    {
        while (!offsets_queue.empty())
        {
            if (offset_states[offsets_queue.front()].active_connection_count == 0)
                throw Exception("Cannot find enough connections to replicas", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
            offsets_queue.pop();
        }
    }

    /// Check if we don't need to listen hedged_connections_factory file descriptor in epoll anymore.
    if (next_replica_in_process && (state == HedgedConnectionsFactory::State::CANNOT_CHOOSE || offsets_queue.empty()))
    {
        epoll.remove(hedged_connections_factory.getFileDescriptor());
        next_replica_in_process = false;
    }
}

void HedgedConnections::finishProcessReplica(ReplicaState & replica, bool disconnect)
{
    LOG_DEBUG(log, "finishProcessReplica");

    epoll.remove(replica.epoll.getFileDescriptor());
    --offset_states[fd_to_replica_location[replica.epoll.getFileDescriptor()].offset].active_connection_count;
    fd_to_replica_location.erase(replica.epoll.getFileDescriptor());
    --active_connection_count;

    if (disconnect)
        replica.connection->disconnect();
    replica.connection = nullptr;
}

}
#endif
