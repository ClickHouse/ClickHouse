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
        replicas.emplace_back(ConnectionEstablisher(shuffled_pools[i].pool, &timeouts, settings, table_to_check.get()));

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
    if (!fallback_to_stale_replicas)
        return State::CANNOT_CHOOSE;

    return setBestUsableReplica(connection_out);
}

void HedgedConnectionsFactory::stopChoosingReplicas()
{
    for (auto & [fd, index] : fd_to_replica_index)
    {
        epoll.remove(fd);
        replicas[index].connection_establisher.cancel();
    }

    for (auto & [fd, index] : timeout_fd_to_replica_index)
    {
        replicas[index].change_replica_timeout.reset();
        epoll.remove(fd);
    }

    fd_to_replica_index.clear();
    timeout_fd_to_replica_index.clear();
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
        if (!replicas[next_index].connection_establisher.isInProcess()
            && !replicas[next_index].connection_establisher.isFinished()
            && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries))
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
        replica.connection_establisher.resume();

        processConnectionEstablisherStage(index);

        if (replica.connection_establisher.isInProcess())
        {
            epoll.add(replica.connection_establisher.getFileDescriptor());
            fd_to_replica_index[replica.connection_establisher.getFileDescriptor()] = index;

            /// Add timeout for changing replica.
            replica.change_replica_timeout.setRelative(timeouts.hedged_connection_timeout);
            epoll.add(replica.change_replica_timeout.getDescriptor());
            timeout_fd_to_replica_index[replica.change_replica_timeout.getDescriptor()] = index;
        }
    }
    while (!replicas[index].connection_establisher.isInProcess() && !replicas[index].is_ready);

    if (replicas[index].is_ready)
        connection_out = replicas[index].connection_establisher.getConnection();

    return index;
}

void HedgedConnectionsFactory::processConnectionEstablisherStage(int index, bool remove_from_epoll)
{
    ReplicaStatus & replica = replicas[index];

    if (replica.connection_establisher.isFinished())
    {
        --replicas_in_process_count;
        ++entries_count;

        if (remove_from_epoll)
            removeReplicaFromEpoll(index);

        if (replica.connection_establisher.getResult().is_usable)
        {
            ++usable_count;
            if (replica.connection_establisher.getResult().is_up_to_date)
                replica.is_ready = true;

            return;
        }

        /// If replica is not usable, we need to save fail message.
        if (!replica.connection_establisher.getFailMessage().empty())
            fail_messages += replica.connection_establisher.getFailMessage() + "\n";
    }
    else if (replica.connection_establisher.isFailed())
        processFailedConnection(index, remove_from_epoll);
}

void HedgedConnectionsFactory::processFailedConnection(int index, bool remove_from_epoll)
{
    ConnectionEstablisher & connection_establisher = replicas[index].connection_establisher;
    
    if (remove_from_epoll)
        removeReplicaFromEpoll(index);

    if (!connection_establisher.getFailMessage().empty())
        fail_messages += connection_establisher.getFailMessage() + "\n";

    ShuffledPool & shuffled_pool = shuffled_pools[index];
    LOG_WARNING(
        log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), connection_establisher.getFailMessage());
    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

    shuffled_pool.error_count = std::min(pool->getMaxErrorCup(), shuffled_pool.error_count + 1);

    if (shuffled_pool.error_count >= max_tries)
    {
        ++failed_pools_count;
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
    }

    --replicas_in_process_count;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::processEpollEvents(bool blocking, Connection *& connection_out)
{
    int event_fd;
    while (!epoll.empty())
    {
        event_fd = getReadyFileDescriptor(blocking);

        if (event_fd == -1)
            return State::NOT_READY;

        if (fd_to_replica_index.contains(event_fd))
        {
            int index = fd_to_replica_index[event_fd];
            processConnectionEstablisherEvent(index, connection_out);

            if (replicas[index].is_ready)
                return State::READY;
            if (replicas[index].connection_establisher.isInProcess())
                continue;
        }
        else if (timeout_fd_to_replica_index.contains(event_fd))
            replicas[timeout_fd_to_replica_index[event_fd]].change_replica_timeout.reset();
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);

        /// We reach this point only if we need to start new connection
        /// (Special timeout expired or one of the previous connections failed).
        int index = startEstablishingNewConnection(connection_out);

        /// Return only if replica is ready.
        if (index != -1 && replicas[index].is_ready)
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

void HedgedConnectionsFactory::removeReplicaFromEpoll(int index)
{
    ReplicaStatus & replica = replicas[index];
    epoll.remove(replica.connection_establisher.getFileDescriptor());
    fd_to_replica_index.erase(replica.connection_establisher.getFileDescriptor());

    replica.change_replica_timeout.reset();
    epoll.remove(replica.change_replica_timeout.getDescriptor());
    timeout_fd_to_replica_index.erase(replica.change_replica_timeout.getDescriptor());
}

void HedgedConnectionsFactory::processConnectionEstablisherEvent(int index, Connection *& connection_out)
{
    replicas[index].connection_establisher.resume();
    processConnectionEstablisherStage(index, true);
    if (replicas[index].is_ready)
        connection_out = replicas[index].connection_establisher.getConnection();
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::setBestUsableReplica(Connection *& connection_out)
{
    std::vector<int> indexes;
    for (size_t i = 0; i != replicas.size(); ++i)
    {
        /// Don't add unusable, failed replicas and replicas that are ready or in process.
        if (!replicas[i].connection_establisher.getResult().entry.isNull()
            && replicas[i].connection_establisher.getResult().is_usable
            && !replicas[i].connection_establisher.isInProcess()
            && !replicas[i].is_ready)
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
            return replicas[lhs].connection_establisher.getResult().staleness < replicas[rhs].connection_establisher.getResult().staleness;
        });

    ++ready_replicas_count;
    replicas[indexes[0]].is_ready = true;
    connection_out = replicas[indexes[0]].connection_establisher.getConnection();
    return State::READY;
}

}
#endif
