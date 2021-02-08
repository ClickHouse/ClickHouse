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
        connection_establishers.emplace_back(shuffled_pools[i].pool, &timeouts, settings, table_to_check.get(), log);

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

    /// Try to start establishing connections with max_entries replicas.
    int index;
    for (size_t i = 0; i != max_entries; ++i)
    {
        index = getNextIndex();
        if (index == -1)
            break;

        ReplicaStatePtr replica = startEstablishingConnection(index);
        if (replica->state == State::READY)
            connections.push_back(replica->connection);
    }

    /// Process connections until we get enough READY connections
    /// (work asynchronously with all connections we started).
    Connection * connection = nullptr;
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
    ReplicaStatePtr replica = nullptr;
    int index = -1;
    
    if (start_new_connection)
    {
        /// Try to start establishing connection to the new replica.
        index = getNextIndex();
        if (index != -1)
        {
            replica = startEstablishingConnection(index);
            if (replica->state == State::READY)
            {
                connection_out = replica->connection;
                return State::READY;
            }
        }
    }

    while (index != -1 || !epoll.empty())
    {
        if (index != -1)
        {
            replica = startEstablishingConnection(index);
            if (replica->state == State::READY)
            {
                connection_out = replica->connection;
                return State::READY;
            }
        }

        if (!processEpollEvents(replica, blocking))
            return State::NOT_READY;

        if (replica->state == State::READY)
        {
            connection_out = replica->connection;
            return State::READY;
        }

        index = getNextIndex();
    }

    /// We reach this point only if there was no free up to date replica.
    /// We will try to use usable replica.

    /// Check if we are not allowed to use usable replicas or there is no even a free usable replica.
    if (!fallback_to_stale_replicas || !canGetNewConnection())
        return State::CANNOT_CHOOSE;

    setBestUsableReplica(replica);
    connection_out = replica->connection;
    return replica->state;
}

void HedgedConnectionsFactory::stopChoosingReplicas()
{
    for (auto & [fd, replica] : fd_to_replica)
    {
        removeTimeoutsFromReplica(replica);
        epoll.remove(fd);
        connection_establishers[replica->index].reset();
        replica->reset();
    }

    fd_to_replica.clear();
}

int HedgedConnectionsFactory::getNextIndex()
{
    /// Check if there is no free replica.
    if (entries_count + indexes_in_process.size() + failed_pools_count >= shuffled_pools.size())
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
        if (indexes_in_process.find(next_index) == indexes_in_process.end() && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries)
            && connection_establishers[next_index].stage != ConnectionEstablisher::Stage::FINISHED)
            finish = true;

        /// If we made a complete round, there is no replica to connect.
        else if (next_index == last_used_index)
            return -1;
    }

    last_used_index = next_index;
    return next_index;
}

HedgedConnectionsFactory::ReplicaStatePtr HedgedConnectionsFactory::startEstablishingConnection(int index)
{
    ReplicaStatePtr replica = createNewReplica();

    do
    {
        ConnectionEstablisher & connection_establisher = connection_establishers[index];

        replica->state = State::NOT_READY;
        replica->index = index;
        indexes_in_process.insert(index);

        connection_establisher.reset();
        connection_establisher.run();

        if (connection_establisher.stage != ConnectionEstablisher::Stage::FAILED)
            replica->connection = &*connection_establisher.result.entry;

        processConnectionEstablisherStage(replica);

        if (replica->state == State::NOT_READY)
        {
            epoll.add(connection_establisher.socket_fd);
            fd_to_replica[connection_establisher.socket_fd] = replica;
            connection_establisher.setActionBeforeDisconnect([&](int fd)
            {
                epoll.remove(fd);
                fd_to_replica.erase(fd);
            });
            addTimeouts(replica);
        }
    }
    while (replica->state == State::EMPTY && (index = getNextIndex()) != -1);

    return replica;
}

void HedgedConnectionsFactory::processConnectionEstablisherStage(ReplicaStatePtr & replica, bool remove_from_epoll)
{
    ConnectionEstablisher & connection_establisher = connection_establishers[replica->index];

    if (connection_establisher.stage == ConnectionEstablisher::Stage::FINISHED)
    {
        indexes_in_process.erase(replica->index);
        ++entries_count;

        if (remove_from_epoll)
        {
            epoll.remove(connection_establisher.socket_fd);
            fd_to_replica.erase(connection_establisher.socket_fd);
        }

        if (connection_establisher.result.is_usable)
        {
            ++usable_count;
            if (connection_establisher.result.is_up_to_date)
            {
                replica->state = State::READY;
                ready_indexes.insert(replica->index);
                return;
            }
        }

        /// This replica is not up to date, we will try to find up to date.
        replica->reset();
    }
    else if (connection_establisher.stage == ConnectionEstablisher::Stage::FAILED)
        processFailedConnection(replica);
}

void HedgedConnectionsFactory::processFailedConnection(ReplicaStatePtr & replica)
{
    ShuffledPool & shuffled_pool = shuffled_pools[replica->index];
    LOG_WARNING(
        log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), connection_establishers[replica->index].fail_message);
    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

    shuffled_pool.error_count = std::min(pool->getMaxErrorCup(), shuffled_pool.error_count + 1);

    if (shuffled_pool.error_count >= max_tries)
    {
        ++failed_pools_count;
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
    }

    std::string & fail_message = connection_establishers[replica->index].fail_message;
    if (!fail_message.empty())
        fail_messages += fail_message + "\n";

    indexes_in_process.erase(replica->index);
    replica->reset();
}

void HedgedConnectionsFactory::addTimeouts(ReplicaStatePtr & replica)
{
    addTimeoutToReplica(ConnectionTimeoutType::RECEIVE_TIMEOUT, replica);

    auto stage = connection_establishers[replica->index].stage;
    if (stage == ConnectionEstablisher::Stage::RECEIVE_HELLO)
        addTimeoutToReplica(ConnectionTimeoutType::RECEIVE_HELLO_TIMEOUT, replica);
    else if (stage == ConnectionEstablisher::Stage::RECEIVE_TABLES_STATUS)
        addTimeoutToReplica(ConnectionTimeoutType::RECEIVE_TABLES_STATUS_TIMEOUT, replica);
}

void HedgedConnectionsFactory::addTimeoutToReplica(ConnectionTimeoutType type, ReplicaStatePtr & replica)
{
    ConnectionTimeoutDescriptorPtr timeout_descriptor = createConnectionTimeoutDescriptor(type, timeouts);
    epoll.add(timeout_descriptor->timer.getDescriptor());
    timeout_fd_to_replica[timeout_descriptor->timer.getDescriptor()] = replica;
    replica->active_timeouts[timeout_descriptor->timer.getDescriptor()] = std::move(timeout_descriptor);
}

void HedgedConnectionsFactory::removeTimeoutsFromReplica(ReplicaStatePtr & replica)
{
    for (auto & [fd, _] : replica->active_timeouts)
    {
        epoll.remove(fd);
        timeout_fd_to_replica.erase(fd);
    }
    replica->active_timeouts.clear();
}

bool HedgedConnectionsFactory::processEpollEvents(ReplicaStatePtr & replica, bool blocking)
{
    int event_fd;
    bool finish = false;
    while (!finish)
    {
        event_fd = getReadyFileDescriptor(blocking);

        /// Check if there is no events.
        if (event_fd == -1)
            return false;

        if (fd_to_replica.find(event_fd) != fd_to_replica.end())
        {
            replica = fd_to_replica[event_fd];
            processReplicaEvent(replica);
            /// Check if replica is ready or we need to try next replica.
            if (replica->state == State::READY || replica->state == State::EMPTY)
                finish = true;
        }
        else if (timeout_fd_to_replica.find(event_fd) != timeout_fd_to_replica.end())
        {
            replica = timeout_fd_to_replica[event_fd];
            processTimeoutEvent(replica, replica->active_timeouts[event_fd]);
            /// Check if we need to try next replica.
            if (replica->state == State::EMPTY)
                finish = true;
        }
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }

    return true;
}

int HedgedConnectionsFactory::getReadyFileDescriptor(bool blocking)
{
    for (auto & [fd, replica] : fd_to_replica)
        if (replica->connection->hasReadPendingData())
            return replica->connection->getSocket()->impl()->sockfd();

    epoll_event event;
    event.data.fd = -1;
    epoll.getManyReady(1, &event, blocking);
    return event.data.fd;
}

void HedgedConnectionsFactory::processReplicaEvent(ReplicaStatePtr & replica)
{
    removeTimeoutsFromReplica(replica);
    connection_establishers[replica->index].run();
    processConnectionEstablisherStage(replica, true);
    if (replica->state == State::NOT_READY)
        addTimeouts(replica);
}

void HedgedConnectionsFactory::processTimeoutEvent(ReplicaStatePtr & replica, ConnectionTimeoutDescriptorPtr timeout_descriptor)
{
    epoll.remove(timeout_descriptor->timer.getDescriptor());
    replica->active_timeouts.erase(timeout_descriptor->timer.getDescriptor());
    timeout_fd_to_replica[timeout_descriptor->timer.getDescriptor()];

    if (timeout_descriptor->type == ConnectionTimeoutType::RECEIVE_TIMEOUT)
    {
        removeTimeoutsFromReplica(replica);
        int fd = replica->connection->getSocket()->impl()->sockfd();
        epoll.remove(fd);
        fd_to_replica.erase(fd);

        ConnectionEstablisher & connection_establisher = connection_establishers[replica->index];
        connection_establisher.fail_message = "Receive timeout expired (" + connection_establisher.result.entry->getDescription() + ")";
        connection_establisher.resetResult();
        connection_establisher.stage = ConnectionEstablisher::Stage::FAILED;
        processFailedConnection(replica);
    }
    else if ((timeout_descriptor->type == ConnectionTimeoutType::RECEIVE_HELLO_TIMEOUT
             || timeout_descriptor->type == ConnectionTimeoutType::RECEIVE_TABLES_STATUS_TIMEOUT)
             && entries_count + indexes_in_process.size() + failed_pools_count < shuffled_pools.size())
        replica = createNewReplica();
}

void HedgedConnectionsFactory::setBestUsableReplica(ReplicaStatePtr & replica)
{
    std::vector<int> indexes(connection_establishers.size());
    for (size_t i = 0; i != indexes.size(); ++i)
        indexes[i] = i;

    /// Remove unusable, failed replicas and replicas that are ready or in process.
    indexes.erase(
        std::remove_if(
            indexes.begin(),
            indexes.end(),
            [&](int i)
            {
                return connection_establishers[i].result.entry.isNull() || !connection_establishers[i].result.is_usable ||
                    indexes_in_process.find(i) != indexes_in_process.end() || ready_indexes.find(i) != ready_indexes.end();
            }),
        indexes.end());

    if (indexes.empty())
    {
        replica->state = State::CANNOT_CHOOSE;
        return;
    }

    /// Sort replicas by staleness.
    std::stable_sort(
        indexes.begin(),
        indexes.end(),
        [&](size_t lhs, size_t rhs)
        {
            return connection_establishers[lhs].result.staleness < connection_establishers[rhs].result.staleness;
        });

    replica->index = indexes[0];
    replica->connection = &*connection_establishers[indexes[0]].result.entry;
    replica->state = State::READY;
    ready_indexes.insert(replica->index);
}

ConnectionTimeoutDescriptorPtr createConnectionTimeoutDescriptor(ConnectionTimeoutType type, const ConnectionTimeouts & timeouts)
{
    Poco::Timespan timeout;
    switch (type)
    {
        case ConnectionTimeoutType::RECEIVE_HELLO_TIMEOUT:
            timeout = timeouts.receive_hello_timeout;
            break;
        case ConnectionTimeoutType::RECEIVE_TABLES_STATUS_TIMEOUT:
            timeout = timeouts.receive_tables_status_timeout;
            break;
        case ConnectionTimeoutType::RECEIVE_DATA_TIMEOUT:
            timeout = timeouts.receive_data_timeout;
            break;
        case ConnectionTimeoutType::RECEIVE_TIMEOUT:
            timeout = timeouts.receive_timeout;
            break;
    }

    ConnectionTimeoutDescriptorPtr timeout_descriptor = std::make_shared<ConnectionTimeoutDescriptor>();
    timeout_descriptor->type = type;
    timeout_descriptor->timer.setRelative(timeout);
    return timeout_descriptor;
}

}
#endif
