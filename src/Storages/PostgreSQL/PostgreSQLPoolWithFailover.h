#pragma once

#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "PostgreSQLConnectionPool.h"


namespace postgres
{

class PoolWithFailover
{

public:
    static constexpr inline auto POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES = 5;
    static constexpr inline auto POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_ADDRESSES = 5;

    PoolWithFailover(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const size_t max_tries_ = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    PoolWithFailover(
        const std::string & database,
        const std::string & host_pattern,
        uint16_t port,
        const std::string & user,
        const std::string & password,
        const size_t max_tries = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
        const size_t max_addresses = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_ADDRESSES);

    PoolWithFailover(const PoolWithFailover & other);

    ConnectionHolderPtr get();


private:
    /// Highest priority is 0, the bigger the number in map, the less the priority
    using Replicas = std::vector<ConnectionPoolPtr>;
    using ReplicasWithPriority = std::map<size_t, Replicas>;

    ReplicasWithPriority replicas_with_priority;
    size_t max_tries;
    std::mutex mutex;
};

using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;

}
