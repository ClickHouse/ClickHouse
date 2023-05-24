#if defined(OS_LINUX)

#include <Client/HedgedConnectionsFactory.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event HedgedRequestsChangeReplica;
    extern const Event DistributedConnectionFailTry;
    extern const Event DistributedConnectionFailAtAll;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int LOGICAL_ERROR;
}

HedgedConnectionsFactory::HedgedConnectionsFactory(
    const ConnectionPoolWithFailoverPtr & pool_,
    const Settings * settings_,
    const ConnectionTimeouts & timeouts_,
    std::shared_ptr<QualifiedTableName> table_to_check_)
    : pool(pool_), settings(settings_), timeouts(timeouts_), table_to_check(table_to_check_), log(&Poco::Logger::get("HedgedConnectionsFactory"))
{
    shuffled_pools = pool->getShuffledPools(settings);
    for (auto shuffled_pool : shuffled_pools)
        replicas.emplace_back(ConnectionEstablisherAsync(shuffled_pool.pool, &timeouts, settings, log, table_to_check.get()));

    max_tries
        = (settings ? size_t{settings->connections_with_failover_max_tries} : size_t{DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES});

    fallback_to_stale_replicas = settings && settings->fallback_to_stale_replicas_for_distributed_queries;
}

HedgedConnectionsFactory::~HedgedConnectionsFactory()
{
    /// Stop anything that maybe in progress,
    /// to avoid interfer with the subsequent connections.
    ///
    /// I.e. some replcas may be in the establishing state,
    /// this means that hedged connection is waiting for TablesStatusResponse,
    /// and if the connection will not be canceled,
    /// then next user of the connection will get TablesStatusResponse,
    /// while this is not the expected package.
    stopChoosingReplicas();

    pool->updateSharedError(shuffled_pools);
}

std::vector<Connection *> HedgedConnectionsFactory::getManyConnections(PoolMode pool_mode)
{
    size_t min_entries = (settings && settings->skip_unavailable_shards) ? 0 : 1;

    size_t max_entries = 1;
    switch (pool_mode)
    {
        case PoolMode::GET_ALL:
        {
            min_entries = shuffled_pools.size();
            max_entries = shuffled_pools.size();
            break;
        }
        case PoolMode::GET_ONE:
        {
            max_entries = 1;
            break;
        }
        case PoolMode::GET_MANY:
        {
            max_entries = settings ? size_t(settings->max_parallel_replicas) : 1;
            break;
        }
    }

    std::vector<Connection *> connections;
    connections.reserve(max_entries);
    Connection * connection = nullptr;

    /// Try to start establishing connections with max_entries replicas.
    for (size_t i = 0; i != max_entries; ++i)
    {
        ++requested_connections_count;
        State state = startNewConnectionImpl(connection);
        if (state == State::READY)
            connections.push_back(connection);
        if (state == State::CANNOT_CHOOSE)
            break;
    }

    /// Process connections until we get enough READY connections
    /// (work asynchronously with all connections we started).
    /// TODO: when we get GET_ALL mode we can start reading packets from ready
    /// TODO: connection as soon as we got it, not even waiting for the others.
    while (connections.size() < max_entries)
    {
        /// Set blocking = true to avoid busy-waiting here.
        auto state = waitForReadyConnectionsImpl(/*blocking = */true, connection);
        if (state == State::READY)
            connections.push_back(connection);
        else if (state == State::CANNOT_CHOOSE)
        {
            if (connections.size() >= min_entries)
                break;

            /// Determine the reason of not enough replicas.
            if (!fallback_to_stale_replicas && up_to_date_count < min_entries)
                throw Exception(
                    "Could not find enough connections to up-to-date replicas. Got: " + std::to_string(connections.size())
                    + ", needed: " + std::to_string(min_entries),
                    DB::ErrorCodes::ALL_REPLICAS_ARE_STALE);
            if (usable_count < min_entries)
                throw NetException(
                    "All connection tries failed. Log: \n\n" + fail_messages + "\n",
                    DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);

            throw Exception("Unknown reason of not enough replicas.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    return connections;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::startNewConnection(Connection *& connection_out)
{
    ++requested_connections_count;
    State state = startNewConnectionImpl(connection_out);
    /// If we cannot start new connection but there are connections in epoll, return NOT_READY.
    if (state == State::CANNOT_CHOOSE && !epoll.empty())
        state = State::NOT_READY;

    return state;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::waitForReadyConnections(Connection *& connection_out)
{
    return waitForReadyConnectionsImpl(false, connection_out);
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::waitForReadyConnectionsImpl(bool blocking, Connection *& connection_out)
{
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
        if (replicas[next_index].connection_establisher.getResult().entry.isNull()
            && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries))
            finish = true;

        /// If we made a complete round, there is no replica to connect.
        else if (next_index == last_used_index)
            return -1;
    }

    last_used_index = next_index;
    return next_index;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::startNewConnectionImpl(Connection *& connection_out)
{
    int index;
    State state;
    do
    {
        index = getNextIndex();
        if (index == -1)
            return State::CANNOT_CHOOSE;

        state = resumeConnectionEstablisher(index, connection_out);
    }
    while (state == State::CANNOT_CHOOSE);

    return state;
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
            State state = resumeConnectionEstablisher(index, connection_out);
            if (state == State::NOT_READY)
                continue;

            /// Connection establishing not in process now, remove all
            /// information about it from epoll.
            removeReplicaFromEpoll(index, event_fd);

            if (state == State::READY)
                return state;
        }
        else if (timeout_fd_to_replica_index.contains(event_fd))
        {
            int index = timeout_fd_to_replica_index[event_fd];
            replicas[index].change_replica_timeout.reset();
            ++shuffled_pools[index].slowdown_count;
            ProfileEvents::increment(ProfileEvents::HedgedRequestsChangeReplica);
        }
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);

        /// We reach this point only if we need to start new connection
        /// (Special timeout expired or one of the previous connections failed).
        /// Return only if replica is ready.
        if (startNewConnectionImpl(connection_out) == State::READY)
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

HedgedConnectionsFactory::State HedgedConnectionsFactory::resumeConnectionEstablisher(int index, Connection *& connection_out)
{
    auto res = replicas[index].connection_establisher.resume();

    if (std::holds_alternative<TryResult>(res))
        return processFinishedConnection(index, std::get<TryResult>(res), connection_out);

    int fd = std::get<int>(res);
    if (!fd_to_replica_index.contains(fd))
        addNewReplicaToEpoll(index, fd);

    return State::NOT_READY;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::processFinishedConnection(int index, TryResult result, Connection *& connection_out)
{
    const std::string & fail_message = replicas[index].connection_establisher.getFailMessage();
    if (!fail_message.empty())
        fail_messages += fail_message + "\n";

    if (!result.entry.isNull())
    {
        ++entries_count;

        if (result.is_usable)
        {
            ++usable_count;
            if (result.is_up_to_date)
            {
                ++up_to_date_count;
                if (!skip_replicas_with_two_level_aggregation_incompatibility || !isTwoLevelAggregationIncompatible(&*result.entry))
                {
                    replicas[index].is_ready = true;
                    ++ready_replicas_count;
                    connection_out = &*result.entry;
                    return State::READY;
                }
            }
        }
    }
    else
    {
        ShuffledPool & shuffled_pool = shuffled_pools[index];
        LOG_WARNING(
            log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), fail_message);
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

        shuffled_pool.error_count = std::min(pool->getMaxErrorCup(), shuffled_pool.error_count + 1);
        shuffled_pool.slowdown_count = 0;

        if (shuffled_pool.error_count >= max_tries)
        {
            ++failed_pools_count;
            ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
        }
    }

    return State::CANNOT_CHOOSE;
}

void HedgedConnectionsFactory::stopChoosingReplicas()
{
    for (auto & [fd, index] : fd_to_replica_index)
    {
        --replicas_in_process_count;
        epoll.remove(fd);
        replicas[index].connection_establisher.cancel();
    }

    for (auto & [timeout_fd, index] : timeout_fd_to_replica_index)
    {
        replicas[index].change_replica_timeout.reset();
        epoll.remove(timeout_fd);
    }

    fd_to_replica_index.clear();
    timeout_fd_to_replica_index.clear();
}

void HedgedConnectionsFactory::addNewReplicaToEpoll(int index, int fd)
{
    ++replicas_in_process_count;
    epoll.add(fd);
    fd_to_replica_index[fd] = index;

    /// Add timeout for changing replica.
    replicas[index].change_replica_timeout.setRelative(timeouts.hedged_connection_timeout);
    epoll.add(replicas[index].change_replica_timeout.getDescriptor());
    timeout_fd_to_replica_index[replicas[index].change_replica_timeout.getDescriptor()] = index;
}

void HedgedConnectionsFactory::removeReplicaFromEpoll(int index, int fd)
{
    --replicas_in_process_count;
    epoll.remove(fd);
    fd_to_replica_index.erase(fd);

    replicas[index].change_replica_timeout.reset();
    epoll.remove(replicas[index].change_replica_timeout.getDescriptor());
    timeout_fd_to_replica_index.erase(replicas[index].change_replica_timeout.getDescriptor());
}

int HedgedConnectionsFactory::numberOfProcessingReplicas() const
{
    if (epoll.empty())
        return 0;

    return requested_connections_count - ready_replicas_count;
}

HedgedConnectionsFactory::State HedgedConnectionsFactory::setBestUsableReplica(Connection *& connection_out)
{
    std::vector<int> indexes;
    for (size_t i = 0; i != replicas.size(); ++i)
    {
        /// Don't add unusable, failed replicas and replicas that are ready or in process.
        TryResult result = replicas[i].connection_establisher.getResult();
        if (!result.entry.isNull()
            && result.is_usable
            && !replicas[i].is_ready
            && (!skip_replicas_with_two_level_aggregation_incompatibility || !isTwoLevelAggregationIncompatible(&*result.entry)))
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

    replicas[indexes[0]].is_ready = true;
    TryResult result = replicas[indexes[0]].connection_establisher.getResult();
    connection_out = &*result.entry;
    return State::READY;
}

bool HedgedConnectionsFactory::isTwoLevelAggregationIncompatible(Connection * connection)
{
    return connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD;
}

}
#endif
