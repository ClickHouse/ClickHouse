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
}

HedgedConnections::HedgedConnections(
    const ConnectionPoolWithFailoverPtr & pool_,
    const Settings & settings_,
    const ConnectionTimeouts & timeouts_,
    const ThrottlerPtr & throttler_,
    PoolMode pool_mode,
    std::shared_ptr<QualifiedTableName> table_to_check_)
    : get_hedged_connections(pool_, &settings_, timeouts_, table_to_check_), settings(settings_), throttler(throttler_), log(&Poco::Logger::get("HedgedConnections"))
{
    std::vector<ReplicaStatePtr> replicas_states = get_hedged_connections.getManyConnections(pool_mode);

    for (size_t i = 0; i != replicas_states.size(); ++i)
    {
        replicas_states[i]->parallel_replica_offset = i;
        replicas_states[i]->connection->setThrottler(throttler_);
        epoll.add(replicas_states[i]->fd);
        fd_to_replica[replicas_states[i]->fd] = replicas_states[i];
        replicas.push_back({std::move(replicas_states[i])});
        active_connections_count_by_offset[i] = 1;
    }

    pipeline_for_new_replicas.add([throttler_](ReplicaStatePtr & replica_){ replica_->connection->setThrottler(throttler_); });
}

void HedgedConnections::Pipeline::add(std::function<void(ReplicaStatePtr & replica)> send_function)
{
    pipeline.push_back(send_function);
}

void HedgedConnections::Pipeline::run(ReplicaStatePtr & replica)
{
    for (auto & send_func : pipeline)
        send_func(replica);
}

void HedgedConnections::sendScalarsData(Scalars & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send scalars data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    auto send_scalars_data = [&data](ReplicaStatePtr & replica) { replica->connection->sendScalarsData(data); };

    for (auto & replicas_with_same_offset : replicas)
        for (auto & replica : replicas_with_same_offset)
            if (replica->isReady())
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

    auto send_external_tables_data = [&data](ReplicaStatePtr & replica) { replica->connection->sendExternalTablesData(data[0]); };

    for (auto & replicas_with_same_offset : replicas)
        for (auto & replica : replicas_with_same_offset)
            if (replica->isReady())
                send_external_tables_data(replica);

    pipeline_for_new_replicas.add(send_external_tables_data);
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

    for (auto & replicas_with_same_offset : replicas)
    {
        for (auto & replica : replicas_with_same_offset)
        {
            if (replica->connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
            {
                disable_two_level_aggregation = true;
                break;
            }
        }
        if (disable_two_level_aggregation)
            break;
    }

    auto send_query = [this, timeouts, query, query_id, stage, client_info, with_pending_data](ReplicaStatePtr & replica)
    {
        Settings modified_settings = this->settings;

        if (this->disable_two_level_aggregation)
        {
            /// Disable two-level aggregation due to version incompatibility.
            modified_settings.group_by_two_level_threshold = 0;
            modified_settings.group_by_two_level_threshold_bytes = 0;
        }

        if (this->replicas.size() > 1)
        {
            modified_settings.parallel_replicas_count = this->replicas.size();
            modified_settings.parallel_replica_offset = replica->parallel_replica_offset;
        }

        replica->connection->sendQuery(timeouts, query, query_id, stage, &modified_settings, &client_info, with_pending_data);
        addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, this->epoll, this->timeout_fd_to_replica, timeouts);
        addTimeoutToReplica(TimerTypes::RECEIVE_DATA_TIMEOUT, replica, this->epoll, this->timeout_fd_to_replica, timeouts);
    };

    for (auto & replicas_with_same_offset : replicas)
        for (auto & replica : replicas_with_same_offset)
            send_query(replica);

    pipeline_for_new_replicas.add(send_query);
    sent_query = true;
}

void HedgedConnections::disconnect()
{
    std::lock_guard lock(cancel_mutex);

    for (auto & replicas_with_same_offset : replicas)
        for (auto & replica : replicas_with_same_offset)
            if (replica->isReady())
                finishProcessReplica(replica, true);

    if (get_hedged_connections.hasEventsInProcess())
    {
        get_hedged_connections.stopChoosingReplicas();
        if (next_replica_in_process)
            epoll.remove(get_hedged_connections.getFileDescriptor());
    }
}

std::string HedgedConnections::dumpAddresses() const
{
    std::lock_guard lock(cancel_mutex);

    std::string addresses;
    bool is_first = true;

    for (const auto & replicas_with_same_offset : replicas)
    {
        for (const auto & replica : replicas_with_same_offset)
        {
            if (replica->isReady())
            {
                addresses += (is_first ? "" : "; ") + replica->connection->getDescription();
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

    for (auto & replicas_with_same_offset : replicas)
        for (auto & replica : replicas_with_same_offset)
            if (replica->isReady())
                replica->connection->sendCancel();

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
        Packet packet = receivePacketImpl();
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

    return receivePacketImpl(std::move(async_callback));
}

Packet HedgedConnections::receivePacketImpl(AsyncCallback async_callback)
{
    int event_fd;
    ReplicaStatePtr replica = nullptr;
    Packet packet;
    bool finish = false;
    while (!finish)
    {
        event_fd = getReadyFileDescriptor(async_callback);

        if (fd_to_replica.find(event_fd) != fd_to_replica.end())
        {
            replica = fd_to_replica[event_fd];
            packet = receivePacketFromReplica(replica, async_callback);
            finish = true;
        }
        else if (timeout_fd_to_replica.find(event_fd) != timeout_fd_to_replica.end())
        {
            replica = timeout_fd_to_replica[event_fd];
            processTimeoutEvent(replica, replica->active_timeouts[event_fd]);
        }
        else if (event_fd == get_hedged_connections.getFileDescriptor())
            tryGetNewReplica();
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }

    return packet;
};

int HedgedConnections::getReadyFileDescriptor(AsyncCallback async_callback)
{
    for (auto & [fd, replica] : fd_to_replica)
        if (replica->connection->hasReadPendingData())
            return replica->fd;

    return epoll.getReady(std::move(async_callback)).data.fd;
}

Packet HedgedConnections::receivePacketFromReplica(ReplicaStatePtr & replica, AsyncCallback async_callback)
{
    Packet packet = replica->connection->receivePacket(std::move(async_callback));
    switch (packet.type)
    {
        case Protocol::Server::Data:
            removeTimeoutsFromReplica(replica, epoll, timeout_fd_to_replica);
            processReceiveData(replica);
            addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, timeout_fd_to_replica, get_hedged_connections.getConnectionTimeouts());
            break;
        case Protocol::Server::Progress:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
            removeTimeoutFromReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, timeout_fd_to_replica);
            addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, timeout_fd_to_replica, get_hedged_connections.getConnectionTimeouts());
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

void HedgedConnections::processReceiveData(ReplicaStatePtr & replica)
{
    /// When we receive first packet of data from replica, we stop working with replicas, that are
    /// responsible for the same offset.
    offsets_with_received_data.insert(replica->parallel_replica_offset);

    for (auto & other_replica : replicas[replica->parallel_replica_offset])
    {
        if (other_replica->isReady() && other_replica != replica)
        {
            other_replica->connection->sendCancel();
            finishProcessReplica(other_replica, true);
        }
    }

    /// If we received data from replicas with all offsets, we need to stop choosing new replicas.
    if (get_hedged_connections.hasEventsInProcess() && offsets_with_received_data.size() == replicas.size())
    {
        get_hedged_connections.stopChoosingReplicas();
        if (next_replica_in_process)
            epoll.remove(get_hedged_connections.getFileDescriptor());
    }
}

void HedgedConnections::processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor)
{
    epoll.remove(timeout_descriptor->getDescriptor());
    replica->active_timeouts.erase(timeout_descriptor->getDescriptor());
    timeout_fd_to_replica.erase(timeout_descriptor->getDescriptor());

    if (timeout_descriptor->getType() == TimerTypes::RECEIVE_TIMEOUT)
    {
        size_t offset = replica->parallel_replica_offset;
        finishProcessReplica(replica, true);

        /// Check if there is no active connections with the same offset.
        if (active_connections_count_by_offset[offset] == 0)
            throw NetException("Receive timeout expired", ErrorCodes::SOCKET_TIMEOUT);
    }
    else if (timeout_descriptor->getType() == TimerTypes::RECEIVE_DATA_TIMEOUT)
    {
        offsets_queue.push(replica->parallel_replica_offset);
        tryGetNewReplica();
    }
}

void HedgedConnections::tryGetNewReplica()
{
    ReplicaStatePtr new_replica = get_hedged_connections.getNextConnection(/*non_blocking*/ true);

    /// Skip replicas that doesn't support two-level aggregation if we didn't disable it in sendQuery.
    while (new_replica->isReady() && !disable_two_level_aggregation
           && new_replica->connection->getServerRevision(get_hedged_connections.getConnectionTimeouts()) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
        new_replica = get_hedged_connections.getNextConnection(/*non_blocking*/ true);

    if (new_replica->isReady())
    {
        new_replica->parallel_replica_offset = offsets_queue.front();
        offsets_queue.pop();
        replicas[new_replica->parallel_replica_offset].push_back(new_replica);
        epoll.add(new_replica->fd);
        fd_to_replica[new_replica->fd] = new_replica;
        ++active_connections_count_by_offset[new_replica->parallel_replica_offset];
        pipeline_for_new_replicas.run(new_replica);
    }
    else if (new_replica->isNotReady() && !next_replica_in_process)
    {
        epoll.add(get_hedged_connections.getFileDescriptor());
        next_replica_in_process = true;
    }

    if (next_replica_in_process && (new_replica->isCannotChoose() || offsets_queue.empty()))
    {
        epoll.remove(get_hedged_connections.getFileDescriptor());
        next_replica_in_process = false;
    }
}

void HedgedConnections::finishProcessReplica(ReplicaStatePtr & replica, bool disconnect)
{
    removeTimeoutsFromReplica(replica, epoll, timeout_fd_to_replica);
    epoll.remove(replica->fd);
    fd_to_replica.erase(replica->fd);
    --active_connections_count_by_offset[replica->parallel_replica_offset];
    if (active_connections_count_by_offset[replica->parallel_replica_offset] == 0)
        active_connections_count_by_offset.erase(replica->parallel_replica_offset);

    if (disconnect)
        replica->connection->disconnect();
    replica->reset();
}

}
#endif
