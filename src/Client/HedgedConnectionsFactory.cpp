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

    replicas_timeouts.resize(shuffled_pools.size());

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
    int index;
    for (size_t i = 0; i != max_entries; ++i)
    {
        index = getNextIndex();
        if (index == -1)
            break;

        auto state = startEstablishingConnection(index, connection);
        if (state == State::READY)
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
    int index = -1;

    if (start_new_connection)
        index = getNextIndex();

    while (index != -1 || !epoll.empty())
    {
        if (index != -1)
        {
            State state = startEstablishingConnection(index, connection_out);
            if (state == State::READY)
                return state;
        }

        State state = processEpollEvents(blocking, connection_out);
        if (state != State::EMPTY)
            return state;

        index = getNextIndex();
    }

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
        removeTimeoutsFromReplica(replica_index);
        epoll.remove(fd);
        connection_establishers[replica_index].reset();
    }

    fd_to_replica_index.clear();
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

HedgedConnectionsFactory::State HedgedConnectionsFactory::startEstablishingConnection(int replica_index, Connection *& connection_out)
{
    State state;
    do
    {
        ConnectionEstablisher & connection_establisher = connection_establishers[replica_index];

        state = State::NOT_READY;
        indexes_in_process.insert(replica_index);

        connection_establisher.reset();
        connection_establisher.run();

        state = processConnectionEstablisherStage(replica_index);

        if (state == State::NOT_READY)
        {
            epoll.add(connection_establisher.socket_fd);
            fd_to_replica_index[connection_establisher.socket_fd] = replica_index;
            connection_establisher.setActionBeforeDisconnect([&](int fd)
            {
                epoll.remove(fd);
                fd_to_replica_index.erase(fd);
            });
            addTimeouts(replica_index);
        }
    }
    while (state == State::EMPTY && (replica_index = getNextIndex()) != -1);

    if (state == State::READY)
        connection_out = &*connection_establishers[replica_index].result.entry;

    return state;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::processConnectionEstablisherStage(int replica_index, bool remove_from_epoll)
{
    ConnectionEstablisher & connection_establisher = connection_establishers[replica_index];

    if (connection_establisher.stage == ConnectionEstablisher::Stage::FINISHED)
    {
        indexes_in_process.erase(replica_index);
        ++entries_count;

        if (remove_from_epoll)
        {
            epoll.remove(connection_establisher.socket_fd);
            fd_to_replica_index.erase(connection_establisher.socket_fd);
        }

        if (connection_establisher.result.is_usable)
        {
            ++usable_count;
            if (connection_establisher.result.is_up_to_date)
            {
                ready_indexes.insert(replica_index);
                return State::READY;
            }
        }

        /// This replica is not up to date, we will try to find up to date.
        return State::EMPTY;
    }
    else if (connection_establisher.stage == ConnectionEstablisher::Stage::FAILED)
    {
        processFailedConnection(replica_index);
        return State::EMPTY;
    }

    return State::NOT_READY;
}

void HedgedConnectionsFactory::processFailedConnection(int replica_index)
{
    ShuffledPool & shuffled_pool = shuffled_pools[replica_index];
    LOG_WARNING(
        log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), connection_establishers[replica_index].fail_message);
    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

    shuffled_pool.error_count = std::min(pool->getMaxErrorCup(), shuffled_pool.error_count + 1);

    if (shuffled_pool.error_count >= max_tries)
    {
        ++failed_pools_count;
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
    }

    std::string & fail_message = connection_establishers[replica_index].fail_message;
    if (!fail_message.empty())
        fail_messages += fail_message + "\n";

    indexes_in_process.erase(replica_index);
}

void HedgedConnectionsFactory::addTimeouts(int replica_index)
{
    addTimeoutToReplica(ConnectionTimeoutType::RECEIVE_TIMEOUT, replica_index);

    auto stage = connection_establishers[replica_index].stage;
    if (stage == ConnectionEstablisher::Stage::RECEIVE_HELLO)
        addTimeoutToReplica(ConnectionTimeoutType::RECEIVE_HELLO_TIMEOUT, replica_index);
    else if (stage == ConnectionEstablisher::Stage::RECEIVE_TABLES_STATUS)
        addTimeoutToReplica(ConnectionTimeoutType::RECEIVE_TABLES_STATUS_TIMEOUT, replica_index);
}

void HedgedConnectionsFactory::addTimeoutToReplica(ConnectionTimeoutType type, int replica_index)
{
    ConnectionTimeoutDescriptorPtr timeout_descriptor = createConnectionTimeoutDescriptor(type, timeouts);
    epoll.add(timeout_descriptor->timer.getDescriptor());
    timeout_fd_to_replica_index[timeout_descriptor->timer.getDescriptor()] = replica_index;
    replicas_timeouts[replica_index][timeout_descriptor->timer.getDescriptor()] = std::move(timeout_descriptor);
}

void HedgedConnectionsFactory::removeTimeoutsFromReplica(int replica_index)
{
    for (auto & [fd, _] : replicas_timeouts[replica_index])
    {
        epoll.remove(fd);
        timeout_fd_to_replica_index.erase(fd);
    }
    replicas_timeouts[replica_index].clear();
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::processEpollEvents(bool blocking, Connection *& connection_out)
{
    int event_fd;
    while (true)
    {
        event_fd = getReadyFileDescriptor(blocking);

        /// Check if there is no events.
        if (event_fd == -1)
            return State::NOT_READY;

        if (fd_to_replica_index.find(event_fd) != fd_to_replica_index.end())
        {
            int replica_index = fd_to_replica_index[event_fd];
            State state = processReplicaEvent(replica_index, connection_out);
            /// Return only if replica is ready or we need to try next replica.
            if (state != State::NOT_READY)
                return state;
        }
        else if (timeout_fd_to_replica_index.find(event_fd) != timeout_fd_to_replica_index.end())
        {
            int replica_index = timeout_fd_to_replica_index[event_fd];
            /// Process received timeout. If retured values is true, we need to try new replica.
            if (processTimeoutEvent(replica_index, replicas_timeouts[replica_index][event_fd]))
                return State::EMPTY;
        }
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }
}

int HedgedConnectionsFactory::getReadyFileDescriptor(bool blocking)
{
    for (auto & [fd, replica_index] : fd_to_replica_index)
        if (connection_establishers[replica_index].result.entry->hasReadPendingData())
            return connection_establishers[replica_index].socket_fd;

    epoll_event event;
    event.data.fd = -1;
    epoll.getManyReady(1, &event, blocking);
    return event.data.fd;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::processReplicaEvent(int replica_index, Connection *& connection_out)
{
    removeTimeoutsFromReplica(replica_index);
    connection_establishers[replica_index].run();
    State state = processConnectionEstablisherStage(replica_index, true);
    if (state == State::NOT_READY)
        addTimeouts(replica_index);
    if (state == State::READY)
        connection_out = &*connection_establishers[replica_index].result.entry;
    return state;
}

bool HedgedConnectionsFactory::processTimeoutEvent(int replica_index, ConnectionTimeoutDescriptorPtr timeout_descriptor)
{
    epoll.remove(timeout_descriptor->timer.getDescriptor());
    replicas_timeouts[replica_index].erase(timeout_descriptor->timer.getDescriptor());
    timeout_fd_to_replica_index[timeout_descriptor->timer.getDescriptor()];

    if (timeout_descriptor->type == ConnectionTimeoutType::RECEIVE_TIMEOUT)
    {
        removeTimeoutsFromReplica(replica_index);
        int fd = connection_establishers[replica_index].socket_fd;
        epoll.remove(fd);
        fd_to_replica_index.erase(fd);

        ConnectionEstablisher & connection_establisher = connection_establishers[replica_index];
        connection_establisher.fail_message = "Receive timeout expired (" + connection_establisher.result.entry->getDescription() + ")";
        connection_establisher.resetResult();
        connection_establisher.stage = ConnectionEstablisher::Stage::FAILED;
        processFailedConnection(replica_index);
        return true;
    }

    /// Return true if we can try to start one more connection.
    return entries_count + indexes_in_process.size() + failed_pools_count < shuffled_pools.size();
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::setBestUsableReplica(Connection *& connection_out)
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
        return State::CANNOT_CHOOSE;

    /// Sort replicas by staleness.
    std::stable_sort(
        indexes.begin(),
        indexes.end(),
        [&](size_t lhs, size_t rhs)
        {
            return connection_establishers[lhs].result.staleness < connection_establishers[rhs].result.staleness;
        });

    ready_indexes.insert(indexes[0]);
    connection_out = &*connection_establishers[indexes[0]].result.entry;
    return State::READY;
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
