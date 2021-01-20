#include <Client/GetHedgedConnections.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
}

GetHedgedConnections::~GetHedgedConnections()
{
    pool->updateSharedError(shuffled_pools);
}

GetHedgedConnections::Replicas GetHedgedConnections::getConnections()
{
    entries_count = 0;
    usable_count = 0;
    failed_pools_count = 0;

    ReplicaStatePtr replica = &first_replica;
    int index = 0;

    while (index != -1 || epoll.size() != 0)
    {
        if (index != -1)
        {
            Action action = startTryGetConnection(index, replica);
            if (action == Action::TRY_NEXT_REPLICA)
            {
                index = getNextIndex(index);
                continue;
            }

            if (action == Action::FINISH)
            {
                swapReplicasIfNeeded();
                return {&first_replica, &second_replica};
            }
        }

        /// Process epoll events
        replica = processEpollEvents();
        if (replica->isReady())
        {
            swapReplicasIfNeeded();
            return {&first_replica, &second_replica};
        }

        index = getNextIndex(index);
    }

    /// We reach this point only if there was no up to date replica

    if (usable_count == 0)
    {
        if (settings && settings->skip_unavailable_shards)
        {
            first_replica.state = State::CANNOT_CHOOSE;
            second_replica.state = State::CANNOT_CHOOSE;
            return {&first_replica, &second_replica};
        }

        throw NetException("All connection tries failed. Log: \n\n" + fail_messages + "\n", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
    }
    if (!fallback_to_stale_replicas)
        throw DB::Exception("Could not find connection to up-to-date replica.", DB::ErrorCodes::ALL_REPLICAS_ARE_STALE);

    setBestUsableReplica(first_replica);
    return {&first_replica, &second_replica};
}

void GetHedgedConnections::chooseSecondReplica()
{
    LOG_DEBUG(log, "choose second replica");

    if (second_replica.isCannotChoose() || second_replica.isReady())
        return;

    int index;
    if (second_replica.isNotReady())
        index = second_replica.index;
    else
        index = first_replica.index;

    while (true)
    {
        if (second_replica.isEmpty())
        {

            index = getNextIndex(index);
            if (index == -1)
                break;

            Action action = startTryGetConnection(index, &second_replica);

            if (action == Action::TRY_NEXT_REPLICA)
                continue;

            /// Second replica is ready or we are waiting for response from it
            return;
        }

        if (!second_replica.isNotReady())
            throw Exception("Second replica state must be 'NOT_READY' before process epoll events", ErrorCodes::LOGICAL_ERROR);

        ReplicaStatePtr replica = processEpollEvents( true);

        if (replica != &second_replica)
            throw Exception("Epoll could return only second replica here", ErrorCodes::LOGICAL_ERROR);

        /// If replica is not empty than it is ready or we are waiting for a response from it
        if (!second_replica.isEmpty())
            return;
    }

    /// There is no up to date replica

    LOG_DEBUG(log, "there is no up to date replica for second replica");

    if (!fallback_to_stale_replicas || usable_count <= 1)
        second_replica.state = State::CANNOT_CHOOSE;
    else
        setBestUsableReplica(second_replica, first_replica.index);
}

void GetHedgedConnections::stopChoosingSecondReplica()
{
    LOG_DEBUG(log, "stop choosing second replica");

    if (!second_replica.isNotReady())
        throw Exception("Can't stop choosing second replica, because it's not in process of choosing", ErrorCodes::LOGICAL_ERROR);

    removeTimeoutsFromReplica(&second_replica, epoll);
    epoll.remove(second_replica.fd);

    try_get_connections[second_replica.index].reset();
    second_replica.reset();
}

int GetHedgedConnections::getNextIndex(int cur_index)
{
    /// Check if there is no more available replicas
    if (cur_index == -1 || entries_count + failed_pools_count >= shuffled_pools.size())
        return -1;

    /// We can work with two replicas simultaneously and they must have different indexes
    int skip_index = -1;
    if (!first_replica.isEmpty())
        skip_index = first_replica.index;
    else if (!second_replica.isEmpty())
        skip_index = second_replica.index;

    bool finish = false;
    int next_index = cur_index;
    while (!finish)
    {
        next_index = (next_index + 1) % shuffled_pools.size();

        /// Check if we can try this replica
        if (next_index != skip_index && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries)
            && try_get_connections[next_index].stage != TryGetConnection::Stage::FINISHED)
            finish = true;

        /// If we made a complete round, there is no replica to connect
        else if (next_index == cur_index)
            return -1;
    }

    LOG_DEBUG(log, "get next index: {}", next_index);

    return next_index;
}

GetHedgedConnections::Action GetHedgedConnections::startTryGetConnection(int index, ReplicaStatePtr replica)
{
    LOG_DEBUG(log, "start try get connection with {} replica", index);
    TryGetConnection & try_get_connection = try_get_connections[index];

    replica->state = State::NOT_READY;
    replica->index = index;

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
        try_get_connection.setEpoll(&epoll);
        addTimeouts(replica);
    }

    return action;
}

GetHedgedConnections::Action
GetHedgedConnections::processTryGetConnectionStage(ReplicaStatePtr replica, bool remove_from_epoll)
{
    LOG_DEBUG(log, "process get connection stage for {} replica", replica->index);
    TryGetConnection & try_get_connection = try_get_connections[replica->index];

    if (try_get_connection.stage == TryGetConnection::Stage::FINISHED)
    {
        LOG_DEBUG(log, "stage: FINISHED");
        ++entries_count;

        if (remove_from_epoll)
            epoll.remove(try_get_connection.socket_fd);

        if (try_get_connection.result.is_usable)
        {
            LOG_DEBUG(log, "replica is usable");
            ++usable_count;
            if (try_get_connection.result.is_up_to_date)
            {
                LOG_DEBUG(log, "replica is up to date, finish get hedged connections");
                replica->state = State::READY;
                return Action::FINISH;
            }

            /// This replica is not up to date, we will try to find up to date
            replica->reset();
            return Action::TRY_NEXT_REPLICA;
        }
    }
    else if (try_get_connection.stage == TryGetConnection::Stage::FAILED)
    {
        LOG_DEBUG(log, "stage: FAILED");
        processFailedConnection(replica);
        return Action::TRY_NEXT_REPLICA;
    }

    LOG_DEBUG(log, "middle stage, process epoll events");

    /// Get connection process is not finished
    return Action::PROCESS_EPOLL_EVENTS;
}

void GetHedgedConnections::processFailedConnection(ReplicaStatePtr replica)
{
    LOG_DEBUG(log, "failed connection with {} replica", replica->index);

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

    replica->reset();
}

void GetHedgedConnections::addTimeouts(ReplicaState * replica)
{
    LOG_DEBUG(log, "add timeouts for {} replica", replica->index);

    addTimeoutToReplica(TimerTypes::RECEIVE_TIMEOUT, replica, epoll, timeouts);

    /// If we haven't connected to second replica yet, set special timeout for it
    if (second_replica.isEmpty())
    {
        auto stage = try_get_connections[replica->index].stage;
        if (stage == TryGetConnection::Stage::RECEIVE_HELLO)
            addTimeoutToReplica(TimerTypes::RECEIVE_HELLO_TIMEOUT, replica, epoll, timeouts);
        else if (stage == TryGetConnection::Stage::RECEIVE_TABLES_STATUS)
            addTimeoutToReplica(TimerTypes::RECEIVE_TABLES_STATUS_TIMEOUT, replica, epoll, timeouts);
    }
}

void GetHedgedConnections::swapReplicasIfNeeded()
{
    if ((!first_replica.isReady() && second_replica.isReady()))
    {
        LOG_DEBUG(log, "swap replicas");
        swapReplicas();
    }
}

GetHedgedConnections::ReplicaStatePtr GetHedgedConnections::processEpollEvents(bool non_blocking)
{
    LOG_DEBUG(log, "process epoll events");
    int event_fd;
    ReplicaStatePtr replica = nullptr;
    bool finish = false;
    while (!finish)
    {
        event_fd = getReadyFileDescriptor(epoll);

        if ((replica = isEventReplica(event_fd)))
            finish = processReplicaEvent(replica, non_blocking);

        else if (auto * timeout_descriptor = isEventTimeout(event_fd, replica))
        {
            processTimeoutEvent(replica, timeout_descriptor);
            finish = true;
        }
        else
            throw Exception("Unknown event from epoll", ErrorCodes::LOGICAL_ERROR);
    }

    LOG_DEBUG(log, "cancel process epoll events");

    return replica;
}

GetHedgedConnections::ReplicaStatePtr GetHedgedConnections::isEventReplica(int event_fd)
{
    if (event_fd == first_replica.fd)
        return &first_replica;

    if (event_fd == second_replica.fd)
        return &second_replica;

    return nullptr;
}

TimerDescriptorPtr GetHedgedConnections::isEventTimeout(int event_fd, ReplicaStatePtr & replica_out)
{
    if (first_replica.active_timeouts.find(event_fd) != first_replica.active_timeouts.end())
    {
        replica_out = &first_replica;
        return first_replica.active_timeouts[event_fd].get();
    }

    if (second_replica.active_timeouts.find(event_fd) != second_replica.active_timeouts.end())
    {
        replica_out = &second_replica;
        return second_replica.active_timeouts[event_fd].get();
    }

    return nullptr;
}

int GetHedgedConnections::getReadyFileDescriptor(Epoll & epoll_, AsyncCallback async_callback)
{
    if (first_replica.connection && first_replica.connection->hasReadPendingData())
        return first_replica.fd;

    if (second_replica.connection && second_replica.connection->hasReadPendingData())
        return second_replica.fd;

    return epoll_.getReady(std::move(async_callback)).data.fd;
}

bool GetHedgedConnections::processReplicaEvent(ReplicaStatePtr replica, bool non_blocking)
{
    LOG_DEBUG(log, "epoll event is {} replica", replica->index);
    removeTimeoutsFromReplica(replica, epoll);
    try_get_connections[replica->index].run();
    Action action = processTryGetConnectionStage(replica, true);
    if (action == Action::PROCESS_EPOLL_EVENTS)
    {
        addTimeouts(replica);
        return non_blocking;
    }

    return true;
}

void GetHedgedConnections::processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor)
{
    LOG_DEBUG(log, "epoll event is timeout for {} replica", replica->index);

    epoll.remove(timeout_descriptor->getDescriptor());
    replica->active_timeouts.erase(timeout_descriptor->getDescriptor());

    if (timeout_descriptor->getType() == TimerTypes::RECEIVE_TIMEOUT)
    {
        LOG_DEBUG(log, "process receive timeout for {} replica", replica->index);
        removeTimeoutsFromReplica(replica, epoll);
        epoll.remove(replica->fd);

        TryGetConnection & try_get_connection = try_get_connections[replica->index];
        try_get_connection.fail_message = "Receive timeout expired (" + try_get_connection.result.entry->getDescription() + ")";
        try_get_connection.resetResult();
        try_get_connection.stage = TryGetConnection::Stage::FAILED;
        processFailedConnection(replica);
    }

    else if (timeout_descriptor->getType() == TimerTypes::RECEIVE_HELLO_TIMEOUT
             || timeout_descriptor->getType() == TimerTypes::RECEIVE_TABLES_STATUS_TIMEOUT)
    {
        if (replica->index == second_replica.index || !second_replica.isEmpty())
            throw Exception(
                "Received timeout to connect with second replica, but current replica is second or second replica is not empty",
                ErrorCodes::LOGICAL_ERROR);
        replica = &second_replica;
    }
}

void GetHedgedConnections::setBestUsableReplica(ReplicaState & replica, int skip_index)
{
    LOG_DEBUG(log, "set best usable replica");

    std::vector<int> indexes(try_get_connections.size());
    for (size_t i = 0; i != indexes.size(); ++i)
        indexes[i] = i;

    /// Remove unusable and failed replicas, skip the replica with skip_index index
    indexes.erase(
        std::remove_if(
            indexes.begin(),
            indexes.end(),
            [&](int i) {
                return try_get_connections[i].result.entry.isNull() || !try_get_connections[i].result.is_usable || i == skip_index;
            }),
        indexes.end());

    if (indexes.empty())
        throw Exception("There is no usable replica to choose", ErrorCodes::LOGICAL_ERROR);

    /// Sort replicas by staleness
    std::stable_sort(indexes.begin(), indexes.end(), [&](size_t lhs, size_t rhs) {
        return try_get_connections[lhs].result.staleness < try_get_connections[rhs].result.staleness;
    });

    replica.index = indexes[0];
    replica.connection = &*try_get_connections[indexes[0]].result.entry;
    replica.state = State::READY;
    replica.fd = replica.connection->getSocket()->impl()->sockfd();
}

void addTimeoutToReplica(int type, GetHedgedConnections::ReplicaStatePtr replica, Epoll & epoll, const ConnectionTimeouts & timeouts)
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
    replica->active_timeouts[timeout_descriptor->getDescriptor()] = std::move(timeout_descriptor);
}

void removeTimeoutsFromReplica(GetHedgedConnections::ReplicaStatePtr replica, Epoll & epoll)
{
    for (auto & [fd, _] : replica->active_timeouts)
        epoll.remove(fd);
    replica->active_timeouts.clear();
}

void removeTimeoutFromReplica(int type, GetHedgedConnections::ReplicaStatePtr replica, Epoll & epoll)
{
    auto it = std::find_if(
        replica->active_timeouts.begin(),
        replica->active_timeouts.end(),
        [type](auto & value){ return value.second->getType() == type; }
    );

    if (it != replica->active_timeouts.end())
    {
        epoll.remove(it->first);
        replica->active_timeouts.erase(it);
    }
}

}
