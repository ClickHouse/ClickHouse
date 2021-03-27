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
    static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;

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
        size_t pool_size = POSTGRESQL_POOL_DEFAULT_SIZE,
        int64_t pool_wait_timeout = -1,
        const size_t max_addresses = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_ADDRESSES,
        const size_t max_tries_ = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

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
