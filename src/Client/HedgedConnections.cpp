#include <Core/Protocol.h>
#if defined(OS_LINUX)

#include <Client/HedgedConnections.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event HedgedRequestsChangeReplica;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_changing_replica_until_first_data_packet;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 connections_with_failover_max_tries;
    extern const SettingsDialect dialect;
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsUInt64 parallel_replica_offset;
    extern const SettingsBool skip_unavailable_shards;
}

namespace ErrorCodes
{
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int LOGICAL_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NOT_IMPLEMENTED;
}

HedgedConnections::HedgedConnections(
    const ConnectionPoolWithFailoverPtr & pool_,
    ContextPtr context_,
    const ConnectionTimeouts & timeouts_,
    const ThrottlerPtr & throttler_,
    PoolMode pool_mode,
    std::shared_ptr<QualifiedTableName> table_to_check_,
    AsyncCallback async_callback,
    GetPriorityForLoadBalancing::Func priority_func)
    : hedged_connections_factory(
          pool_,
          context_->getSettingsRef(),
          timeouts_,
          context_->getSettingsRef()[Setting::connections_with_failover_max_tries].value,
          context_->getSettingsRef()[Setting::fallback_to_stale_replicas_for_distributed_queries].value,
          context_->getSettingsRef()[Setting::max_parallel_replicas].value,
          context_->getSettingsRef()[Setting::skip_unavailable_shards].value,
          table_to_check_,
          priority_func)
    , context(std::move(context_))
    , settings(context->getSettingsRef())
    , throttler(throttler_)
{
    std::vector<Connection *> connections = hedged_connections_factory.getManyConnections(pool_mode, std::move(async_callback));

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send scalars data: query not yet sent.");

    auto send_scalars_data = [&data](ReplicaState & replica) { replica.connection->sendScalarsData(data); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_scalars_data(replica);

    pipeline_for_new_replicas.add(send_scalars_data);
}

void HedgedConnections::sendQueryPlan(const QueryPlan & query_plan)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send query plan: query not yet sent.");

    auto send_query_plan = [&query_plan](ReplicaState & replica) { replica.connection->sendQueryPlan(query_plan); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_query_plan(replica);

    pipeline_for_new_replicas.add(send_query_plan);
}

void HedgedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send external tables data: query not yet sent.");

    if (data.size() != size())
        throw Exception(ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES, "Mismatch between replicas and data sources");

    auto send_external_tables_data = [&](ReplicaState & replica)
    {
        size_t offset = fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset;
        replica.connection->sendExternalTablesData(data[offset]);
    };

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send uuids after query is sent.");

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
    ClientInfo & client_info,
    bool with_pending_data,
    const std::vector<String> & external_roles)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query already sent.");

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

    auto send_query = [this, timeouts, query, query_id, stage, client_info, with_pending_data, external_roles](ReplicaState & replica)
    {
        Settings modified_settings = settings;

        /// Queries in foreign languages are transformed to ClickHouse-SQL. Ensure the setting before sending.
        modified_settings[Setting::dialect] = Dialect::clickhouse;
        modified_settings[Setting::dialect].changed = false;

        if (disable_two_level_aggregation)
        {
            /// Disable two-level aggregation due to version incompatibility.
            modified_settings[Setting::group_by_two_level_threshold] = 0;
            modified_settings[Setting::group_by_two_level_threshold_bytes] = 0;
        }

        const bool enable_offset_parallel_processing = context->canUseOffsetParallelReplicas();

        if (offset_states.size() > 1 && enable_offset_parallel_processing)
        {
            modified_settings[Setting::parallel_replicas_count] = offset_states.size();
            modified_settings[Setting::parallel_replica_offset] = fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset;
        }

        /// FIXME: Remove once we will make `allow_experimental_analyzer` obsolete setting.
        /// Make the analyzer being set, so it will be effectively applied on the remote server.
        /// In other words, the initiator always controls whether the analyzer enabled or not for
        /// all servers involved in the distributed query processing.
        modified_settings.set("allow_experimental_analyzer", static_cast<bool>(modified_settings[Setting::allow_experimental_analyzer]));

        replica.connection->sendQuery(
            timeouts, query, /* query_parameters */ {}, query_id, stage, &modified_settings, &client_info, with_pending_data, external_roles, {});

        replica.change_replica_timeout.setRelative(timeouts.receive_data_timeout);
        replica.packet_receiver->setTimeout(hedged_connections_factory.getConnectionTimeouts().receive_timeout);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot cancel. Either no query sent or already cancelled.");

    /// All hedged connections should be stopped, since otherwise before the
    /// HedgedConnectionsFactory will be destroyed (that will happen from
    /// QueryPipeline dtor) they could still do some work.
    /// And not only this does not make sense, but it also could lead to
    /// use-after-free of the current_thread, since the thread from which they
    /// had been created differs from the thread where the dtor of
    /// QueryPipeline will be called and the initial thread could be already
    /// destroyed (especially when the system is under pressure).
    if (hedged_connections_factory.hasEventsInProcess())
        hedged_connections_factory.stopChoosingReplicas();

    cancelled = true;

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            if (replica.connection)
                replica.connection->sendCancel();
}

Packet HedgedConnections::drain()
{
    std::lock_guard lock(cancel_mutex);

    if (!cancelled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot drain connections: cancel first.");

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

UInt64 HedgedConnections::receivePacketTypeUnlocked(AsyncCallback)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'receivePacketTypeUnlocked()' not implemented for HedgedConnections");
}

Packet HedgedConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot receive packets: no query sent.");
    if (!hasActiveConnections())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No more packets are available.");

    if (epoll.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No pending events in epoll.");

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
            offsets_queue.push(static_cast<int>(location.offset));
            ProfileEvents::increment(ProfileEvents::HedgedRequestsChangeReplica);
            startNewReplica();
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown event from epoll");
    }
}

bool HedgedConnections::resumePacketReceiver(const HedgedConnections::ReplicaLocation & location)
{
    ReplicaState & replica_state = offset_states[location.offset].replicas[location.index];
    replica_state.packet_receiver->resume();

    if (replica_state.packet_receiver->isPacketReady())
    {
        /// Reset the socket timeout after some packet received
        replica_state.packet_receiver->setTimeout(hedged_connections_factory.getConnectionTimeouts().receive_timeout);
        last_received_packet = replica_state.packet_receiver->getPacket();
        return true;
    }
    if (replica_state.packet_receiver->isTimeoutExpired())
    {
        const String & description = replica_state.connection->getDescription();
        finishProcessReplica(replica_state, true);

        /// Check if there is no more active connections with the same offset and there is no new replica in process.
        if (offset_states[location.offset].active_connection_count == 0 && !offset_states[location.offset].next_replica_in_process)
            throw NetException(
                ErrorCodes::SOCKET_TIMEOUT,
                "Timeout exceeded while reading from socket ({}, receive timeout {} ms)",
                description,
                replica_state.packet_receiver->getTimeout().totalMilliseconds());
    }
    else if (replica_state.packet_receiver->hasException())
    {
        finishProcessReplica(replica_state, true);
        std::rethrow_exception(replica_state.packet_receiver->getException());
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
        events_count = epoll.getManyReady(1, &event, blocking ? -1 : 0);
        if (!events_count && async_callback)
            async_callback(epoll.getFileDescriptor(), 0, AsyncEventTimeoutType::NONE, epoll.getDescription(), AsyncTaskExecutor::Event::READ | AsyncTaskExecutor::Event::ERROR);
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
                if (settings[Setting::allow_changing_replica_until_first_data_packet] && !replica.is_change_replica_timeout_expired)
                    replica.change_replica_timeout.setRelative(hedged_connections_factory.getConnectionTimeouts().receive_data_timeout);
                else
                    disableChangingReplica(replica_location);
            }
            replica_with_last_received_packet = replica_location;
            break;
        case Protocol::Server::TimezoneUpdate:
        case Protocol::Server::PartUUIDs:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
        case Protocol::Server::ProfileEvents:
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

    if (cancelled)
    {
        /// Do not start new connection if query is already canceled.
        if (connection)
            connection->disconnect();

        state = HedgedConnectionsFactory::State::CANNOT_CHOOSE;
    }

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
                    throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot find enough connections to replicas");
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

void HedgedConnections::setAsyncCallback(AsyncCallback async_callback)
{
    for (auto & offset_status : offset_states)
    {
        for (auto & replica : offset_status.replicas)
        {
            if (replica.connection)
                replica.connection->setAsyncCallback(async_callback);
        }
    }
}

}
#endif
