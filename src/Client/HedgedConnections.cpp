#if defined(OS_LINUX)

#include <Client/HedgedConnections.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/ClientInfo.h>

namespace ProfileEvents
{
    extern const Event HedgedRequestsChangeReplica;
}

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

    offset_states.reserve(connections.size());
    for (size_t i = 0; i != connections.size(); ++i)
    {
        offset_states.emplace_back();
        offset_states[i].replicas.emplace_back(connections[i]);
        offset_states[i].active_connection_count = 1;

        ReplicaState & replica = offset_states[i].replicas.back();
        replica.connection->setThrottler(throttler_);

        epoll.add(replica.packet_receiver->getFileDescriptor());
        fd_to_replica_location[replica.packet_receiver->getFileDescriptor()] = ReplicaLocation{i, 0};

        epoll.add(replica.change_replica_timeout.getDescriptor());
        timeout_fd_to_replica_location[replica.change_replica_timeout.getDescriptor()] = ReplicaLocation{i, 0};
    }

    active_connection_count = connections.size();
    offsets_with_disabled_changing_replica = 0;
    pipeline_for_new_replicas.add([throttler_](ReplicaState & replica_) { replica_.connection->setThrottler(throttler_); });
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

    if (!disable_two_level_aggregation)
    {
        /// Tell hedged_connections_factory to skip replicas that doesn't support two-level aggregation.
        hedged_connections_factory.skipReplicasWithTwoLevelAggregationIncompatibility();
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
            modified_settings.parallel_replica_offset = fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset;
        }

        replica.connection->sendQuery(timeouts, query, query_id, stage, &modified_settings, &client_info, with_pending_data);
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
        if (hedged_connections_factory.numberOfProcessingReplicas() > 0)
            epoll.remove(hedged_connections_factory.getFileDescriptor());

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

    ReplicaLocation location = getReadyReplicaLocation(std::move(async_callback));
    return receivePacketFromReplica(location);
}

HedgedConnections::ReplicaLocation HedgedConnections::getReadyReplicaLocation(AsyncCallback async_callback)
{
    /// Firstly, resume replica with the last received packet if it has pending data.
    if (replica_with_last_received_packet)
    {
        ReplicaLocation location = replica_with_last_received_packet.value();
        replica_with_last_received_packet.reset();
        if (offset_states[location.offset].replicas[location.index].connection->hasReadPendingData() && resumePacketReceiver(location))
            return location;
    }

    int event_fd;
    while (true)
    {
        /// Get ready file descriptor from epoll and process it.
        event_fd = getReadyFileDescriptor(async_callback);

        if (event_fd == hedged_connections_factory.getFileDescriptor())
            checkNewReplica();
        else if (fd_to_replica_location.contains(event_fd))
        {
            ReplicaLocation location = fd_to_replica_location[event_fd];
            if (resumePacketReceiver(location))
                return location;
        }
        else if (timeout_fd_to_replica_location.contains(event_fd))
        {
            ReplicaLocation location = timeout_fd_to_replica_location[event_fd];
            offset_states[location.offset].replicas[location.index].change_replica_timeout.reset();
            offset_states[location.offset].replicas[location.index].is_change_replica_timeout_expired = true;
            offset_states[location.offset].next_replica_in_process = true;
            offsets_queue.push(location.offset);
            ProfileEvents::increment(ProfileEvents::HedgedRequestsChangeReplica);
            startNewReplica();
        }
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }
};

bool HedgedConnections::resumePacketReceiver(const HedgedConnections::ReplicaLocation & location)
{
    ReplicaState & replica_state = offset_states[location.offset].replicas[location.index];
    auto res = replica_state.packet_receiver->resume();

    if (std::holds_alternative<Packet>(res))
    {
        last_received_packet = std::move(std::get<Packet>(res));
        return true;
    }
    else if (std::holds_alternative<Poco::Timespan>(res))
    {
        finishProcessReplica(replica_state, true);

        /// Check if there is no more active connections with the same offset and there is no new replica in process.
        if (offset_states[location.offset].active_connection_count == 0 && !offset_states[location.offset].next_replica_in_process)
            throw NetException("Receive timeout expired", ErrorCodes::SOCKET_TIMEOUT);
    }

    return false;
}

int HedgedConnections::getReadyFileDescriptor(AsyncCallback async_callback)
{
    epoll_event event;
    event.data.fd = -1;
    size_t events_count = 0;
    bool blocking = !static_cast<bool>(async_callback);
    while (events_count == 0)
    {
        events_count = epoll.getManyReady(1, &event, blocking);
        if (!events_count && async_callback)
            async_callback(epoll.getFileDescriptor(), 0, epoll.getDescription());
    }
    return event.data.fd;
}

Packet HedgedConnections::receivePacketFromReplica(const ReplicaLocation & replica_location)
{
    ReplicaState & replica = offset_states[replica_location.offset].replicas[replica_location.index];
    Packet packet = std::move(last_received_packet);
    switch (packet.type)
    {
        case Protocol::Server::Data:
            /// If we received the first not empty data packet and still can change replica,
            /// disable changing replica with this offset.
            if (offset_states[replica_location.offset].can_change_replica && packet.block.rows() > 0)
                disableChangingReplica(replica_location);
            replica_with_last_received_packet = replica_location;
            break;
        case Protocol::Server::Progress:
            /// Check if we have made some progress and still can change replica.
            if (offset_states[replica_location.offset].can_change_replica && packet.progress.read_bytes > 0)
            {
                /// If we are allowed to change replica until the first data packet,
                /// just restart timeout (if it hasn't expired yet). Otherwise disable changing replica with this offset.
                if (settings.allow_changing_replica_until_first_data_packet && !replica.is_change_replica_timeout_expired)
                    replica.change_replica_timeout.setRelative(hedged_connections_factory.getConnectionTimeouts().receive_data_timeout);
                else
                    disableChangingReplica(replica_location);
            }
            replica_with_last_received_packet = replica_location;
            break;
        case Protocol::Server::PartUUIDs:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
            replica_with_last_received_packet = replica_location;
            break;

        case Protocol::Server::EndOfStream:
            /// Check case when we receive EndOfStream before first not empty data packet
            /// or positive progress. It may happen if max_parallel_replicas > 1 and
            /// there is no way to sample data in this query.
            if (offset_states[replica_location.offset].can_change_replica)
                disableChangingReplica(replica_location);
            finishProcessReplica(replica, false);
            break;

        case Protocol::Server::Exception:
        default:
            /// Check case when we receive Exception before first not empty data packet
            /// or positive progress. It may happen if max_parallel_replicas > 1 and
            /// there is no way to sample data in this query.
            if (offset_states[replica_location.offset].can_change_replica)
                disableChangingReplica(replica_location);
            finishProcessReplica(replica, true);
            break;
    }

    return packet;
}

void HedgedConnections::disableChangingReplica(const ReplicaLocation & replica_location)
{
    /// Stop working with replicas, that are responsible for the same offset.
    OffsetState & offset_state = offset_states[replica_location.offset];
    offset_state.replicas[replica_location.index].change_replica_timeout.reset();
    ++offsets_with_disabled_changing_replica;
    offset_state.can_change_replica = false;

    for (size_t i = 0; i != offset_state.replicas.size(); ++i)
    {
        if (i != replica_location.index && offset_state.replicas[i].connection)
        {
            offset_state.replicas[i].connection->sendCancel();
            finishProcessReplica(offset_state.replicas[i], true);
        }
    }

    /// If we disabled changing replica with all offsets, we need to stop choosing new replicas.
    if (hedged_connections_factory.hasEventsInProcess() && offsets_with_disabled_changing_replica == offset_states.size())
    {
        if (hedged_connections_factory.numberOfProcessingReplicas() > 0)
            epoll.remove(hedged_connections_factory.getFileDescriptor());
        hedged_connections_factory.stopChoosingReplicas();
    }
}

void HedgedConnections::startNewReplica()
{
    Connection * connection = nullptr;
    HedgedConnectionsFactory::State state = hedged_connections_factory.startNewConnection(connection);

    /// Check if we need to add hedged_connections_factory file descriptor to epoll.
    if (state == HedgedConnectionsFactory::State::NOT_READY && hedged_connections_factory.numberOfProcessingReplicas() == 1)
        epoll.add(hedged_connections_factory.getFileDescriptor());

    processNewReplicaState(state, connection);
}

void HedgedConnections::checkNewReplica()
{
    Connection * connection = nullptr;
    HedgedConnectionsFactory::State state = hedged_connections_factory.waitForReadyConnections(connection);

    processNewReplicaState(state, connection);

    /// Check if we don't need to listen hedged_connections_factory file descriptor in epoll anymore.
    if (hedged_connections_factory.numberOfProcessingReplicas() == 0)
        epoll.remove(hedged_connections_factory.getFileDescriptor());
}

void HedgedConnections::processNewReplicaState(HedgedConnectionsFactory::State state, Connection * connection)
{
    switch (state)
    {
        case HedgedConnectionsFactory::State::READY:
        {
            size_t offset = offsets_queue.front();
            offsets_queue.pop();

            offset_states[offset].replicas.emplace_back(connection);
            ++offset_states[offset].active_connection_count;
            offset_states[offset].next_replica_in_process = false;
            ++active_connection_count;

            ReplicaState & replica = offset_states[offset].replicas.back();
            epoll.add(replica.packet_receiver->getFileDescriptor());
            fd_to_replica_location[replica.packet_receiver->getFileDescriptor()] = ReplicaLocation{offset, offset_states[offset].replicas.size() - 1};
            epoll.add(replica.change_replica_timeout.getDescriptor());
            timeout_fd_to_replica_location[replica.change_replica_timeout.getDescriptor()] = ReplicaLocation{offset, offset_states[offset].replicas.size() - 1};

            pipeline_for_new_replicas.run(replica);
            break;
        }
        case HedgedConnectionsFactory::State::CANNOT_CHOOSE:
        {
            while (!offsets_queue.empty())
            {
                /// Check if there is no active replica with needed offsets.
                if (offset_states[offsets_queue.front()].active_connection_count == 0)
                    throw Exception("Cannot find enough connections to replicas", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
                offset_states[offsets_queue.front()].next_replica_in_process = false;
                offsets_queue.pop();
            }
            break;
        }
        case HedgedConnectionsFactory::State::NOT_READY:
            break;
    }
}

void HedgedConnections::finishProcessReplica(ReplicaState & replica, bool disconnect)
{
    /// It's important to remove file descriptor from epoll exactly before cancelling packet_receiver,
    /// because otherwise another thread can try to receive a packet, get this file descriptor
    /// from epoll and resume cancelled packet_receiver.
    epoll.remove(replica.packet_receiver->getFileDescriptor());
    epoll.remove(replica.change_replica_timeout.getDescriptor());

    replica.packet_receiver->cancel();
    replica.change_replica_timeout.reset();

    --offset_states[fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset].active_connection_count;
    fd_to_replica_location.erase(replica.packet_receiver->getFileDescriptor());
    timeout_fd_to_replica_location.erase(replica.change_replica_timeout.getDescriptor());

    --active_connection_count;

    if (disconnect)
        replica.connection->disconnect();
    replica.connection = nullptr;
}

}
#endif
