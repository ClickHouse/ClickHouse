#if defined(OS_LINUX)

#include <Client/HedgedConnectionsFactory.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int ALL_REPLICAS_ARE_STALE;
}

HedgedConnectionsFactory::HedgedConnectionsFactory(
    const ConnectionPoolWithFailoverPtr & pool_,
    const Settings * settings_,
    const ConnectionTimeouts & timeouts_,
    std::shared_ptr<QualifiedTableName> table_to_check_)
    : pool(pool_), settings(settings_), timeouts(timeouts_), table_to_check(table_to_check_), log(&Poco::Logger::get("HedgedConnectionsFactory"))
{
    shuffled_pools = pool->getShuffledPools(settings);
    for (size_t i = 0; i != shuffled_pools.size(); ++i)
    {
        ConnectionEstablisher establisher(shuffled_pools[i].pool, &timeouts, settings, table_to_check.get(), log);
        replicas.emplace_back(std::move(establisher));
    }

    max_tries
        = (settings ? size_t{settings->connections_with_failover_max_tries} : size_t{DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES});

    fallback_to_stale_replicas = settings && settings->fallback_to_stale_replicas_for_distributed_queries;
    entries_count = 0;
    usable_count = 0;
    failed_pools_count = 0;
}

HedgedConnectionsFactory::~HedgedConnectionsFactory()
{
    pool->updateSharedError(shuffled_pools);
}

std::vector<Connection *> HedgedConnectionsFactory::getManyConnections(PoolMode pool_mode)
{
    size_t min_entries = (settings && settings->skip_unavailable_shards) ? 0 : 1;

    size_t max_entries;
    if (pool_mode == PoolMode::GET_ALL)
    {
        min_entries = shuffled_pools.size();
        max_entries = shuffled_pools.size();
    }
    else if (pool_mode == PoolMode::GET_ONE)
        max_entries = 1;
    else if (pool_mode == PoolMode::GET_MANY)
        max_entries = settings ? size_t(settings->max_parallel_replicas) : 1;
    else
        throw DB::Exception("Unknown pool allocation mode", DB::ErrorCodes::LOGICAL_ERROR);

    std::vector<Connection *> connections;
    connections.reserve(max_entries);
    Connection * connection = nullptr;

    /// Try to start establishing connections with max_entries replicas.
    for (size_t i = 0; i != max_entries; ++i)
    {
        int index = startEstablishingNewConnection(connection);
        if (index == -1)
            break;
        if (replicas[index].is_ready)
            connections.push_back(connection);
    }

    /// Process connections until we get enough READY connections
    /// (work asynchronously with all connections we started).
    while (connections.size() < max_entries)
    {
        auto state = getNextConnection(false, true, connection);
        if (state == State::READY)
            connections.push_back(connection);
        else if (state == State::CANNOT_CHOOSE)
        {
            if (connections.size() >= min_entries)
                break;

            /// Determine the reason of not enough replicas.
            if (!fallback_to_stale_replicas && usable_count >= min_entries)
                throw DB::Exception(
                    "Could not find enough connections to up-to-date replicas. Got: " + std::to_string(connections.size())
                    + ", needed: " + std::to_string(min_entries),
                    DB::ErrorCodes::ALL_REPLICAS_ARE_STALE);

            throw DB::NetException(
                "All connection tries failed. Log: \n\n" + fail_messages + "\n",
                DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
        }
    }

    return connections;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::getNextConnection(bool start_new_connection, bool blocking, Connection *& connection_out)
{
    if (start_new_connection)
    {
        int index = startEstablishingNewConnection(connection_out);
        if (index != -1 && replicas[index].is_ready)
            return State::READY;
    }

    State state = processEpollEvents(blocking, connection_out);
    if (state != State::CANNOT_CHOOSE)
        return state;

    /// We reach this point only if there was no free up to date replica.
    /// We will try to use usable replica.

    /// Check if we are not allowed to use usable replicas or there is no even a free usable replica.
    if (!fallback_to_stale_replicas || !canGetNewConnection())
        return State::CANNOT_CHOOSE;

    return setBestUsableReplica(connection_out);
}

void HedgedConnectionsFactory::stopChoosingReplicas()
{
    for (auto & [fd, replica_index] : fd_to_replica_index)
    {
        resetReplicaTimeouts(replica_index);
        epoll.remove(fd);
        replicas[replica_index].connection_establisher.reset();
    }

    fd_to_replica_index.clear();
}

int HedgedConnectionsFactory::getNextIndex()
{
    /// Check if there is no free replica.
    if (entries_count + replicas_in_process_count + failed_pools_count >= shuffled_pools.size())
        return -1;

    /// Check if it's the first time.
    if (last_used_index == -1)
    {
        last_used_index = 0;
        return 0;
    }

    bool finish = false;
    int next_index = last_used_index;
    while (!finish)
    {
        next_index = (next_index + 1) % shuffled_pools.size();

        /// Check if we can try this replica.
        if (!replicas[next_index].is_in_process && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries)
            && replicas[next_index].connection_establisher.stage != ConnectionEstablisher::Stage::FINISHED)
            finish = true;

        /// If we made a complete round, there is no replica to connect.
        else if (next_index == last_used_index)
            return -1;
    }

    last_used_index = next_index;
    return next_index;
}

int HedgedConnectionsFactory::startEstablishingNewConnection(Connection *& connection_out)
{
    int index;
    do
    {
        index = getNextIndex();
        if (index == -1)
            return -1;

        ReplicaStatus & replica = replicas[index];

        ++replicas_in_process_count;
        replica.is_in_process = true;
        replica.connection_establisher.reset();
        replica.connection_establisher.run();

        processConnectionEstablisherStage(index);

        if (replica.is_in_process)
        {
            replica.epoll.add(replica.connection_establisher.socket_fd);
            replica.connection_establisher.setActionBeforeDisconnect([&](int fd){ replica.epoll.remove(fd); });
            addTimeouts(index);
            epoll.add(replica.epoll.getFileDescriptor());
            fd_to_replica_index[replica.epoll.getFileDescriptor()] = index;
        }
    }
    while (!replicas[index].is_ready && !replicas[index].is_in_process);

    if (replicas[index].is_ready)
        connection_out = &*replicas[index].connection_establisher.result.entry;

    return index;
}

void HedgedConnectionsFactory::processConnectionEstablisherStage(int replica_index, bool remove_from_epoll)
{
    ReplicaStatus & replica = replicas[replica_index];

    if (replica.connection_establisher.stage == ConnectionEstablisher::Stage::FINISHED)
    {
        replica.is_in_process = false;
        --replicas_in_process_count;
        ++entries_count;

        if (remove_from_epoll)
        {
            epoll.remove(replica.epoll.getFileDescriptor());
            fd_to_replica_index.erase(replica.epoll.getFileDescriptor());
        }

        if (replica.connection_establisher.result.is_usable)
        {
            ++usable_count;
            if (replica.connection_establisher.result.is_up_to_date)
            {
                ++ready_replicas_count;
                replica.is_ready = true;
                return;
            }
        }
        else
        {
            std::string & fail_message = replica.connection_establisher.fail_message;
            if (!fail_message.empty())
                fail_messages += fail_message + "\n";
        }
    }
    else if (replica.connection_establisher.stage == ConnectionEstablisher::Stage::FAILED)
        processFailedConnection(replica_index, remove_from_epoll);
}

void HedgedConnectionsFactory::processFailedConnection(int replica_index, bool remove_from_epoll)
{
    if (remove_from_epoll)
    {
        epoll.remove(replicas[replica_index].epoll.getFileDescriptor());
        fd_to_replica_index.erase(replicas[replica_index].epoll.getFileDescriptor());
    }

    std::string & fail_message = replicas[replica_index].connection_establisher.fail_message;
    if (!fail_message.empty())
        fail_messages += fail_message + "\n";

    ShuffledPool & shuffled_pool = shuffled_pools[replica_index];
    LOG_WARNING(
        log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), fail_message);
    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

    shuffled_pool.error_count = std::min(pool->getMaxErrorCup(), shuffled_pool.error_count + 1);

    if (shuffled_pool.error_count >= max_tries)
    {
        ++failed_pools_count;
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
    }

    --replicas_in_process_count;
    replicas[replica_index].is_in_process = false;
}

void HedgedConnectionsFactory::addTimeouts(int replica_index)
{
    auto stage = replicas[replica_index].connection_establisher.stage;
    if (stage == ConnectionEstablisher::Stage::RECEIVE_HELLO)
    {
        replicas[replica_index].receive_timeout.setRelative(timeouts.receive_timeout);
        replicas[replica_index].change_replica_timeout.setRelative(timeouts.receive_hello_timeout);
    }
    else if (stage == ConnectionEstablisher::Stage::RECEIVE_TABLES_STATUS)
    {
        replicas[replica_index].receive_timeout.setRelative(Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0));
        replicas[replica_index].change_replica_timeout.setRelative(timeouts.receive_tables_status_timeout);
    }
}

void HedgedConnectionsFactory::resetReplicaTimeouts(int replica_index)
{
    replicas[replica_index].receive_timeout.reset();
    replicas[replica_index].change_replica_timeout.reset();
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::processEpollEvents(bool blocking, Connection *& connection_out)
{
    int event_fd;
    while (!epoll.empty())
    {
        /// Firstly, check connections for pending data.
        int replica_index = checkPendingData();
        if (replica_index != -1)
        {
            processSocketEvent(replica_index, connection_out);
            /// Return only if replica is ready.
            if (replicas[replica_index].is_ready)
                return State::READY;

            continue;
        }

        /// Get ready descriptor fro epoll.
        event_fd = getReadyFileDescriptor(blocking);

        /// Check if there is no events.
        if (event_fd == -1)
            return State::NOT_READY;

        if (!fd_to_replica_index.contains(event_fd))
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);

        replica_index = fd_to_replica_index[event_fd];

        /// Read all events from replica epoll.
        /// If socket is ready and timeout is alarmed simultaneously, skip timeout.
        bool is_socket_ready = false;
        bool is_receive_timeout_alarmed = false;
        bool is_change_replica_timeout_alarmed = false;

        epoll_event events[3];
        events[0].data.fd = events[1].data.fd = events[2].data.fd = -1;
        size_t ready_count = replicas[replica_index].epoll.getManyReady(3, events, true);
        for (size_t i = 0; i != ready_count; ++i)
        {
            if (events[i].data.fd == replicas[replica_index].connection_establisher.socket_fd)
                is_socket_ready = true;
            if (events[i].data.fd == replicas[replica_index].receive_timeout.getDescriptor())
                is_receive_timeout_alarmed = true;
            if (events[i].data.fd == replicas[replica_index].change_replica_timeout.getDescriptor())
                is_change_replica_timeout_alarmed = true;
        }

        if (is_socket_ready)
        {
            processSocketEvent(replica_index, connection_out);
            /// Return only if replica is ready.
            if (replicas[replica_index].is_ready)
                return State::READY;
            if (replicas[replica_index].is_in_process)
                continue;
        }
        else
        {
            if (is_receive_timeout_alarmed)
                processReceiveTimeout(replica_index);

            if (is_change_replica_timeout_alarmed)
                replicas[replica_index].change_replica_timeout.reset();
        }

        /// We reach this point only if we need to start new connection.
        replica_index = startEstablishingNewConnection(connection_out);
        /// Return only if replica is ready.
        if (replica_index != -1 && replicas[replica_index].is_ready)
            return State::READY;
    }

    return State::CANNOT_CHOOSE;
}

int HedgedConnectionsFactory::getReadyFileDescriptor(bool blocking)
{
    epoll_event event;
    event.data.fd = -1;
    epoll.getManyReady(1, &event, blocking);
    return event.data.fd;
}

int HedgedConnectionsFactory::checkPendingData()
{
    for (auto & [fd, replica_index] : fd_to_replica_index)
        if (replicas[replica_index].connection_establisher.result.entry->hasReadPendingData())
            return replica_index;

    return -1;
}

void HedgedConnectionsFactory::processSocketEvent(int replica_index, Connection *& connection_out)
{
    resetReplicaTimeouts(replica_index);
    replicas[replica_index].connection_establisher.run();
    processConnectionEstablisherStage(replica_index, true);
    if (replicas[replica_index].is_in_process)
        addTimeouts(replica_index);
    if (replicas[replica_index].is_ready)
        connection_out = &*replicas[replica_index].connection_establisher.result.entry;
}

void HedgedConnectionsFactory::processReceiveTimeout(int replica_index)
{
    resetReplicaTimeouts(replica_index);
    ReplicaStatus & replica = replicas[replica_index];

    replica.connection_establisher.fail_message =
        "Code: 209, e.displayText() = DB::NetException: Timeout exceeded while reading from socket (" + replica.connection_establisher.result.entry->getDescription() + ")";
    replica.connection_establisher.resetResult();
    replica.connection_establisher.stage = ConnectionEstablisher::Stage::FAILED;
    processFailedConnection(replica_index, true);
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::setBestUsableReplica(Connection *& connection_out)
{
    std::vector<int> indexes;
    for (size_t i = 0; i != replicas.size(); ++i)
    {
        /// Don't add unusable, failed replicas and replicas that are ready or in process.
        if (!replicas[i].connection_establisher.result.entry.isNull() && replicas[i].connection_establisher.result.is_usable &&
            !replicas[i].is_in_process && !replicas[i].is_ready)
            indexes.push_back(i);
    }

    if (indexes.empty())
        return State::CANNOT_CHOOSE;

    /// Sort replicas by staleness.
    std::stable_sort(
        indexes.begin(),
        indexes.end(),
        [&](size_t lhs, size_t rhs)
        {
            return replicas[lhs].connection_establisher.result.staleness < replicas[rhs].connection_establisher.result.staleness;
        });

    ++ready_replicas_count;
    replicas[indexes[0]].is_ready = true;
    connection_out = &*replicas[indexes[0]].connection_establisher.result.entry;
    return State::READY;
}

}
#endif
