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
    std::shared_ptr<QualifiedTableName> table_to_check_)
    : get_hedged_connections(pool_, &settings_, timeouts_, table_to_check_), settings(settings_), throttler(throttler_), log(&Poco::Logger::get("HedgedConnections"))
{
    replicas = get_hedged_connections.getConnections();

    /// First replica may have state CANNOT_CHOOSE if setting skip_unavailable_shards is enabled
    if (replicas.first_replica->isReady())
        replicas.first_replica->connection->setThrottler(throttler);

    if (!replicas.second_replica->isCannotChoose())
    {
        if (replicas.second_replica->isNotReady())
            epoll.add(get_hedged_connections.getFileDescriptor());

        auto set_throttler = [this, throttler_](ReplicaStatePtr replica)
        {
            replica->connection->setThrottler(throttler_);
        };
        second_replica_pipeline.add(std::function<void(ReplicaStatePtr)>(set_throttler));
    }
}

void HedgedConnections::Pipeline::add(std::function<void(ReplicaStatePtr replica)> send_function)
{
    pipeline.push_back(send_function);
}

void HedgedConnections::Pipeline::run(ReplicaStatePtr replica)
{
    for (auto & send_func : pipeline)
        send_func(replica);

    pipeline.clear();
}

size_t HedgedConnections::size() const
{
    if (replicas.first_replica->isReady() || replicas.second_replica->isReady())
        return 1;

    return 0;
}

bool HedgedConnections::hasActiveConnections() const
{
    return replicas.first_replica->isReady() || replicas.second_replica->isReady();
}

void HedgedConnections::sendScalarsData(Scalars & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send scalars data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    auto send_scalars_data = [&data](ReplicaStatePtr replica) { replica->connection->sendScalarsData(data); };

    if (replicas.first_replica->isReady())
        send_scalars_data(replicas.first_replica);

    if (replicas.second_replica->isReady())
        send_scalars_data(replicas.second_replica);
    else if (!replicas.second_replica->isCannotChoose())
        second_replica_pipeline.add(std::function<void(ReplicaStatePtr)>(send_scalars_data));
}

void HedgedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

    if (data.size() != size())
        throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

    auto send_external_tables_data = [&data](ReplicaStatePtr replica) { replica->connection->sendExternalTablesData(data[0]); };

    if (replicas.first_replica->isReady())
        send_external_tables_data(replicas.first_replica);

    if (replicas.second_replica->isReady())
        send_external_tables_data(replicas.second_replica);
    else if (!replicas.second_replica->isCannotChoose())
        second_replica_pipeline.add(send_external_tables_data);
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

    auto send_query = [this, timeouts, query, query_id, stage, client_info, with_pending_data](ReplicaStatePtr replica)
    {
        Settings modified_settings = settings;
        if (replica->connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
        {
            modified_settings.group_by_two_level_threshold = 0;
            modified_settings.group_by_two_level_threshold_bytes = 0;
        }

        replica->connection->sendQuery(timeouts, query, query_id, stage, &modified_settings, &client_info, with_pending_data);
        this->epoll.add(replica->fd);
        addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, this->epoll, timeouts);
    };

    if (replicas.first_replica->isReady())
    {
        send_query(replicas.first_replica);
        if (replicas.second_replica->isEmpty())
            addTimeoutToReplica(TimerTypes::RECEIVE_DATA_TIMEOUT, replicas.first_replica, epoll, timeouts);
    }

    if (replicas.second_replica->isReady())
        send_query(replicas.second_replica);
    else if (!replicas.second_replica->isCannotChoose())
        second_replica_pipeline.add(send_query);

    sent_query = true;
}

void HedgedConnections::disconnect()
{
    std::lock_guard lock(cancel_mutex);

    if (replicas.first_replica->isReady())
    {
        replicas.first_replica->connection->disconnect();
        replicas.first_replica->reset();
    }

    if (replicas.second_replica->isReady())
    {
        replicas.second_replica->connection->disconnect();
        replicas.second_replica->reset();
    }
    else if (replicas.second_replica->isNotReady())
        get_hedged_connections.stopChoosingSecondReplica();
}

std::string HedgedConnections::dumpAddresses() const
{
    std::lock_guard lock(cancel_mutex);

    std::string addresses = "";

    if (replicas.first_replica->isReady())
        addresses += replicas.first_replica->connection->getDescription();

    if (replicas.second_replica->isReady())
        addresses += "; " + replicas.second_replica->connection->getDescription();

    return addresses;
}

void HedgedConnections::sendCancel()
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query || cancelled)
        throw Exception("Cannot cancel. Either no query sent or already cancelled.", ErrorCodes::LOGICAL_ERROR);

    if (replicas.first_replica->isReady())
        replicas.first_replica->connection->sendCancel();

    if (replicas.second_replica->isReady())
        replicas.second_replica->connection->sendCancel();

    cancelled = true;
}


Packet HedgedConnections::drain()
{
    std::lock_guard lock(cancel_mutex);

    if (!cancelled)
        throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

    Packet res;
    res.type = Protocol::Server::EndOfStream;

    while (epoll.size() != 0)
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
    return receivePacketUnlocked();
}

Packet HedgedConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    if (!sent_query)
        throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
    if (!hasActiveConnections())
        throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

    if (epoll.size() == 0)
        throw Exception("No pending events in epoll.", ErrorCodes::LOGICAL_ERROR);

    return receivePacketImpl(std::move(async_callback));
}

Packet HedgedConnections::receivePacketImpl(AsyncCallback async_callback)
{
    int event_fd;
    ReplicaStatePtr replica;
    Packet packet;
    bool finish = false;
    while (!finish)
    {
        event_fd = get_hedged_connections.getReadyFileDescriptor(epoll, async_callback);

        if (auto timeout_descriptor = get_hedged_connections.isEventTimeout(event_fd, replica))
            processTimeoutEvent(replica, timeout_descriptor);
        else if ((replica = get_hedged_connections.isEventReplica(event_fd)))
        {
            packet = receivePacketFromReplica(replica, async_callback);
            finish = true;
        }
        else if (event_fd == get_hedged_connections.getFileDescriptor())
            processGetHedgedConnectionsEvent();
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }

    return packet;
};

Packet HedgedConnections::receivePacketFromReplica(ReplicaStatePtr replica, AsyncCallback async_callback)
{
    Packet packet = replica->connection->receivePacket(std::move(async_callback));
    switch (packet.type)
    {
        case Protocol::Server::Data:
            removeTimeoutsFromReplica(replica, epoll);
            processReceiveData(replica);
            addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, get_hedged_connections.getConnectionTimeouts());
            break;
        case Protocol::Server::Progress:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
            removeTimeoutFromReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll);
            addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, get_hedged_connections.getConnectionTimeouts());
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

void HedgedConnections::processReceiveData(ReplicaStatePtr replica)
{
    /// When we receive first packet of data from any replica, we continue working with this replica
    /// and stop working with another replica (if there is another replica). If current replica is
    /// second, move it to the first place.
    if (replica == replicas.second_replica)
        get_hedged_connections.swapReplicas();

    if (replicas.second_replica->isCannotChoose() || replicas.second_replica->isEmpty())
        return;

    if (replicas.second_replica->isNotReady())
    {
        get_hedged_connections.stopChoosingSecondReplica();
        epoll.remove(get_hedged_connections.getFileDescriptor());
    }
    else if (replicas.second_replica->isReady())
    {
        replicas.second_replica->connection->sendCancel();
        finishProcessReplica(replicas.second_replica, true);
    }
}

void HedgedConnections::processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor)
{
    epoll.remove(timeout_descriptor->getDescriptor());
    replica->active_timeouts.erase(timeout_descriptor->getDescriptor());

    if (timeout_descriptor->getType() == TimerTypes::RECEIVE_TIMEOUT)
    {
        finishProcessReplica(replica, true);

        if (!replicas.first_replica->isReady() && !replicas.second_replica->isNotReady())
            throw NetException("Receive timeout expired", ErrorCodes::SOCKET_TIMEOUT);
    }
    else if (timeout_descriptor->getType() == TimerTypes::RECEIVE_DATA_TIMEOUT)
    {
        if (!replicas.second_replica->isEmpty())
            throw Exception("Cannot start choosing second replica, it's not empty", ErrorCodes::LOGICAL_ERROR);

        get_hedged_connections.chooseSecondReplica();

        if (replicas.second_replica->isReady())
            processChosenSecondReplica();
        else if (replicas.second_replica->isNotReady())
            epoll.add(get_hedged_connections.getFileDescriptor());
    }
}

void HedgedConnections::processGetHedgedConnectionsEvent()
{
    get_hedged_connections.chooseSecondReplica();
    if (replicas.second_replica->isReady())
        processChosenSecondReplica();

    if (!replicas.second_replica->isNotReady())
        epoll.remove(get_hedged_connections.getFileDescriptor());
}

void HedgedConnections::processChosenSecondReplica()
{
    second_replica_pipeline.run(replicas.second_replica);

    /// In case when the first replica get receive timeout before the second is chosen,
    /// we need to move the second replica to the first place
    get_hedged_connections.swapReplicasIfNeeded();
}

void HedgedConnections::finishProcessReplica(ReplicaStatePtr replica, bool disconnect)
{
    removeTimeoutsFromReplica(replica, epoll);
    epoll.remove(replica->fd);
    if (disconnect)
        replica->connection->disconnect();
    replica->reset();

    /// Move active connection to the first replica if it exists
    get_hedged_connections.swapReplicasIfNeeded();
}

}
