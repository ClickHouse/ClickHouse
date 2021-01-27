#include <Client/GetHedgedConnections.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

GetHedgedConnections::GetHedgedConnections(
    const ConnectionPoolWithFailoverPtr & pool_,
    const Settings * settings_,
    const ConnectionTimeouts & timeouts_,
    std::shared_ptr<QualifiedTableName> table_to_check_)
    : pool(pool_), settings(settings_), timeouts(timeouts_), table_to_check(table_to_check_)
{
    log = &Poco::Logger::get("GetHedgedConnections");
    shuffled_pools = pool->getShuffledPools(settings);
    for (size_t i = 0; i != shuffled_pools.size(); ++i)
        try_get_connections.emplace_back(shuffled_pools[i].pool, &timeouts, settings, table_to_check, log);

    max_tries
        = (settings ? size_t{settings->connections_with_failover_max_tries} : size_t{DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES});

    fallback_to_stale_replicas = settings ? settings->fallback_to_stale_replicas_for_distributed_queries : false;
    entries_count = 0;
    usable_count = 0;
    failed_pools_count = 0;
}

GetHedgedConnections::~GetHedgedConnections()
{
    pool->updateSharedError(shuffled_pools);
}

std::vector<GetHedgedConnections::ReplicaStatePtr> GetHedgedConnections::getManyConnections(PoolMode pool_mode)
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

    std::vector<ReplicaStatePtr> replicas;
    replicas.reserve(max_entries);
    for (size_t i = 0; i != max_entries; ++i)
    {
        auto replica = getNextConnection(false);
        if (replica->isCannotChoose())
        {
            if (replicas.size() >= min_entries)
                break;

            /// Determine the reason of not enough replicas.
            if (!fallback_to_stale_replicas && usable_count >= min_entries)
                throw DB::Exception(
                    "Could not find enough connections to up-to-date replicas. Got: " + std::to_string(replicas.size())
                    + ", needed: " + std::to_string(min_entries),
                    DB::ErrorCodes::ALL_REPLICAS_ARE_STALE);

            throw DB::NetException(
                "Could not connect to " + std::to_string(min_entries) + " replicas. Log: \n\n" + fail_messages + "\n",
                DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
        }
        replicas.push_back(replica);
    }

    return replicas;
}

GetHedgedConnections::ReplicaStatePtr GetHedgedConnections::getNextConnection(bool non_blocking)
{
//    LOG_DEBUG(log, "getNextConnection");
    ReplicaStatePtr replica = createNewReplica();

    int index;

    /// Check if it's the first time.
    if (epoll.size() == 0 && ready_indexes.size() == 0)
    {
        index = 0;
        last_used_index = 0;
    }
    else
        index = getNextIndex();

    bool is_first = true;

    while (index != -1 || epoll.size() != 0)
    {
        if (index == -1 && !is_first && non_blocking)
        {
            replica->state = State::NOT_READY;
            return replica;
        }

        if (is_first)
            is_first = false;

        if (index != -1)
        {
            Action action = startTryGetConnection(index, replica);

            if (action == Action::FINISH)
                return replica;

            if (action == Action::TRY_NEXT_REPLICA)
            {
                index = getNextIndex();
                continue;
            }

            if (action == Action::PROCESS_EPOLL_EVENTS && non_blocking)
                return replica;
        }

        replica = processEpollEvents(non_blocking);
        if (replica->isReady() || (replica->isNotReady() && non_blocking))
            return replica;

        if (replica->isNotReady())
            throw Exception("Not ready replica after processing epoll events.", ErrorCodes::LOGICAL_ERROR);

        index = getNextIndex();
    }

    /// We reach this point only if there was no free up to date replica.

    /// Check if there is no even a free usable replica
    if (!canGetNewConnection())
    {
        replica->state = State::CANNOT_CHOOSE;
        return replica;
    }

    if (!fallback_to_stale_replicas)
    {
        replica->state = State::CANNOT_CHOOSE;
        return replica;
    }

    setBestUsableReplica(replica);
    return replica;
}

void GetHedgedConnections::stopChoosingReplicas()
{
//    LOG_DEBUG(log, "stopChoosingReplicas");
    for (auto & [fd, replica] : fd_to_replica)
    {
        removeTimeoutsFromReplica(replica, epoll, timeout_fd_to_replica);
        epoll.remove(fd);
        try_get_connections[replica->index].reset();
        replica->reset();
    }

    fd_to_replica.clear();
}

int GetHedgedConnections::getNextIndex()
{
    /// Check if there is no more available replicas
    if (entries_count + failed_pools_count >= shuffled_pools.size())
        return -1;

    bool finish = false;
    int next_index = last_used_index;
    while (!finish)
    {
        next_index = (next_index + 1) % shuffled_pools.size();

        /// Check if we can try this replica
        if (indexes_in_process.find(next_index) == indexes_in_process.end() && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries)
            && try_get_connections[next_index].stage != TryGetConnection::Stage::FINISHED)
            finish = true;

        /// If we made a complete round, there is no replica to connect
        else if (next_index == last_used_index)
            return -1;
    }

//    LOG_DEBUG(log, "get next index: {}", next_index);

    last_used_index = next_index;
    return next_index;
}

GetHedgedConnections::Action GetHedgedConnections::startTryGetConnection(int index, ReplicaStatePtr & replica)
{
//    LOG_DEBUG(log, "start try get connection with {} replica", index);
    TryGetConnection & try_get_connection = try_get_connections[index];

    replica->state = State::NOT_READY;
    replica->index = index;
    indexes_in_process.insert(index);

    try_get_connection.reset();
    try_get_connection.run();

    if (try_get_connection.stage != TryGetConnection::Stage::FAILED)
    {
        replica->fd = try_get_connection.socket_fd;
        replica->connection = &*try_get_connection.result.entry;
    }

    Action action = processTryGetConnectionStage(replica);

    if (action == Action::PROCESS_EPOLL_EVENTS)
    {
        epoll.add(try_get_connection.socket_fd);
        fd_to_replica[try_get_connection.socket_fd] = replica;
        try_get_connection.setActionBeforeDisconnect(
            [&](int fd)
            {
                epoll.remove(fd);
                fd_to_replica.erase(fd);
            });
        addTimeouts(replica);
    }

    return action;
}

GetHedgedConnections::Action
GetHedgedConnections::processTryGetConnectionStage(ReplicaStatePtr & replica, bool remove_from_epoll)
{
//    LOG_DEBUG(log, "process get connection stage for {} replica", replica->index);
    TryGetConnection & try_get_connection = try_get_connections[replica->index];

    if (try_get_connection.stage == TryGetConnection::Stage::FINISHED)
    {
        indexes_in_process.erase(replica->index);

//        LOG_DEBUG(log, "stage: FINISHED");
        ++entries_count;

        if (remove_from_epoll)
        {
            epoll.remove(try_get_connection.socket_fd);
            fd_to_replica.erase(try_get_connection.socket_fd);
        }

        if (try_get_connection.result.is_usable)
        {
//            LOG_DEBUG(log, "replica is usable");
            ++usable_count;
            if (try_get_connection.result.is_up_to_date)
            {
//                LOG_DEBUG(log, "replica is up to date, finish get hedged connections");
                replica->state = State::READY;
                ready_indexes.insert(replica->index);
                return Action::FINISH;
            }
        }

        /// This replica is not up to date, we will try to find up to date
        fd_to_replica.erase(replica->fd);
        replica->reset();
        return Action::TRY_NEXT_REPLICA;
    }
    else if (try_get_connection.stage == TryGetConnection::Stage::FAILED)
    {
//        LOG_DEBUG(log, "stage: FAILED");
        processFailedConnection(replica);
        return Action::TRY_NEXT_REPLICA;
    }

//    LOG_DEBUG(log, "middle stage, process epoll events");

    /// Get connection process is not finished
    return Action::PROCESS_EPOLL_EVENTS;
}

void GetHedgedConnections::processFailedConnection(ReplicaStatePtr & replica)
{
//    LOG_DEBUG(log, "failed connection with {} replica", replica->index);

    ShuffledPool & shuffled_pool = shuffled_pools[replica->index];
    LOG_WARNING(
        log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), try_get_connections[replica->index].fail_message);
    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

    shuffled_pool.error_count = std::min(pool->getMaxErrorCup(), shuffled_pool.error_count + 1);

    if (shuffled_pool.error_count >= max_tries)
    {
        ++failed_pools_count;
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
    }

    std::string & fail_message = try_get_connections[replica->index].fail_message;
    if (!fail_message.empty())
        fail_messages += fail_message + "\n";

    indexes_in_process.erase(replica->index);
    replica->reset();
}

void GetHedgedConnections::addTimeouts(ReplicaStatePtr & replica)
{
//    LOG_DEBUG(log, "add timeouts for {} replica", replica->index);

    addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, timeout_fd_to_replica, timeouts);

    auto stage = try_get_connections[replica->index].stage;
    if (stage == TryGetConnection::Stage::RECEIVE_HELLO)
        addTimeoutToReplica(TimerTypes::RECEIVE_HELLO_TIMEOUT, replica, epoll, timeout_fd_to_replica, timeouts);
    else if (stage == TryGetConnection::Stage::RECEIVE_TABLES_STATUS)
        addTimeoutToReplica(TimerTypes::RECEIVE_TABLES_STATUS_TIMEOUT, replica, epoll, timeout_fd_to_replica, timeouts);
}

GetHedgedConnections::ReplicaStatePtr GetHedgedConnections::processEpollEvents(bool non_blocking)
{
//    LOG_DEBUG(log, "process epoll events");
    int event_fd;
    ReplicaStatePtr replica = nullptr;
    bool finish = false;
    while (!finish)
    {
        event_fd = getReadyFileDescriptor();

        if (fd_to_replica.find(event_fd) != fd_to_replica.end())
        {
            replica = fd_to_replica[event_fd];
            finish = processReplicaEvent(replica, non_blocking);
        }
        else if (timeout_fd_to_replica.find(event_fd) != timeout_fd_to_replica.end())
        {
            replica = timeout_fd_to_replica[event_fd];
            finish = processTimeoutEvent(replica, replica->active_timeouts[event_fd].get(), non_blocking);
        }
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }

//    LOG_DEBUG(log, "cancel process epoll events");

    return replica;
}

int GetHedgedConnections::getReadyFileDescriptor(AsyncCallback async_callback)
{
    for (auto & [fd, replica] : fd_to_replica)
        if (replica->connection->hasReadPendingData())
            return replica->fd;

    return epoll.getReady(std::move(async_callback)).data.fd;
}

bool GetHedgedConnections::processReplicaEvent(ReplicaStatePtr & replica, bool non_blocking)
{
//    LOG_DEBUG(log, "epoll event is {} replica", replica->index);
    removeTimeoutsFromReplica(replica, epoll, timeout_fd_to_replica);
    try_get_connections[replica->index].run();
    Action action = processTryGetConnectionStage(replica, true);
    if (action == Action::PROCESS_EPOLL_EVENTS)
    {
        addTimeouts(replica);
        return non_blocking;
    }

    return true;
}

bool GetHedgedConnections::processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor, bool non_blocking)
{
//    LOG_DEBUG(log, "epoll event is timeout for {} replica", replica->index);

    epoll.remove(timeout_descriptor->getDescriptor());
    replica->active_timeouts.erase(timeout_descriptor->getDescriptor());
    timeout_fd_to_replica[timeout_descriptor->getDescriptor()];

    if (timeout_descriptor->getType() == TimerTypes::RECEIVE_TIMEOUT)
    {
//        LOG_DEBUG(log, "process receive timeout for {} replica", replica->index);
        removeTimeoutsFromReplica(replica, epoll, timeout_fd_to_replica);
        epoll.remove(replica->fd);
        fd_to_replica.erase(replica->fd);

        TryGetConnection & try_get_connection = try_get_connections[replica->index];
        try_get_connection.fail_message = "Receive timeout expired (" + try_get_connection.result.entry->getDescription() + ")";
        try_get_connection.resetResult();
        try_get_connection.stage = TryGetConnection::Stage::FAILED;
        processFailedConnection(replica);

        return true;
    }

    else if ((timeout_descriptor->getType() == TimerTypes::RECEIVE_HELLO_TIMEOUT
             || timeout_descriptor->getType() == TimerTypes::RECEIVE_TABLES_STATUS_TIMEOUT)
             && entries_count + ready_indexes.size() + failed_pools_count < shuffled_pools.size())
    {
        replica = createNewReplica();
        return true;
    }

    return non_blocking;
}

void GetHedgedConnections::setBestUsableReplica(ReplicaStatePtr & replica)
{
//    LOG_DEBUG(log, "set best usable replica");

    std::vector<int> indexes(try_get_connections.size());
    for (size_t i = 0; i != indexes.size(); ++i)
        indexes[i] = i;

    /// Remove unusable and failed replicas, skip ready replicas
    indexes.erase(
        std::remove_if(
            indexes.begin(),
            indexes.end(),
            [&](int i) {
                return try_get_connections[i].result.entry.isNull() || !try_get_connections[i].result.is_usable ||
                    indexes_in_process.find(i) != indexes_in_process.end() || ready_indexes.find(i) != ready_indexes.end();
            }),
        indexes.end());

    if (indexes.empty())
    {
        replica->state = State::CANNOT_CHOOSE;
        return;
    }

    /// Sort replicas by staleness
    std::stable_sort(indexes.begin(), indexes.end(), [&](size_t lhs, size_t rhs) {
        return try_get_connections[lhs].result.staleness < try_get_connections[rhs].result.staleness;
    });

    replica->index = indexes[0];
    replica->connection = &*try_get_connections[indexes[0]].result.entry;
    replica->state = State::READY;
    replica->fd = replica->connection->getSocket()->impl()->sockfd();
    ready_indexes.insert(replica->index);
}

void addTimeoutToReplica(
    int type,
    GetHedgedConnections::ReplicaStatePtr & replica,
    Epoll & epoll,
    std::unordered_map<int, GetHedgedConnections::ReplicaStatePtr> & timeout_fd_to_replica,
    const ConnectionTimeouts & timeouts)
{
    Poco::Timespan timeout;
    switch (type)
    {
        case TimerTypes::RECEIVE_HELLO_TIMEOUT:
            timeout = timeouts.receive_hello_timeout;
            break;
        case TimerTypes::RECEIVE_TABLES_STATUS_TIMEOUT:
            timeout = timeouts.receive_tables_status_timeout;
            break;
        case TimerTypes::RECEIVE_DATA_TIMEOUT:
            timeout = timeouts.receive_data_timeout;
            break;
        case TimerTypes::RECEIVE_TIMEOUT:
            timeout = timeouts.receive_timeout;
            break;
        default:
            throw Exception("Unknown timeout type", ErrorCodes::BAD_ARGUMENTS);
    }

    std::unique_ptr<TimerDescriptor> timeout_descriptor = std::make_unique<TimerDescriptor>();
    timeout_descriptor->setType(type);
    timeout_descriptor->setRelative(timeout);
    epoll.add(timeout_descriptor->getDescriptor());
    timeout_fd_to_replica[timeout_descriptor->getDescriptor()] = replica;
    replica->active_timeouts[timeout_descriptor->getDescriptor()] = std::move(timeout_descriptor);
}

void removeTimeoutsFromReplica(
    GetHedgedConnections::ReplicaStatePtr & replica,
    Epoll & epoll,
    std::unordered_map<int, GetHedgedConnections::ReplicaStatePtr> & timeout_fd_to_replica)
{
    for (auto & [fd, _] : replica->active_timeouts)
    {
        epoll.remove(fd);
        timeout_fd_to_replica.erase(fd);
    }
    replica->active_timeouts.clear();
}

void removeTimeoutFromReplica(
    int type,
    GetHedgedConnections::ReplicaStatePtr & replica,
    Epoll & epoll,
    std::unordered_map<int, GetHedgedConnections::ReplicaStatePtr> & timeout_fd_to_replica)
{
    auto it = std::find_if(
        replica->active_timeouts.begin(),
        replica->active_timeouts.end(),
        [type](auto & value){ return value.second->getType() == type; }
    );

    if (it != replica->active_timeouts.end())
    {
        epoll.remove(it->first);
        timeout_fd_to_replica.erase(it->first);
        replica->active_timeouts.erase(it);
    }
}

}
