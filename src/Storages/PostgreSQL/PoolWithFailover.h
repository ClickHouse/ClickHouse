#pragma once

#include <mutex>
#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/PostgreSQL/ConnectionHolder.h>


namespace postgres
{

class PoolWithFailover
{

using RemoteDescription = std::vector<std::pair<String, uint16_t>>;

public:
    static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;
    static constexpr inline auto POSTGRESQL_POOL_WAIT_TIMEOUT = 5000;
    static constexpr inline auto POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES = 5;

    PoolWithFailover(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        size_t pool_size = POSTGRESQL_POOL_DEFAULT_SIZE,
        size_t pool_wait_timeout = POSTGRESQL_POOL_WAIT_TIMEOUT,
        size_t max_tries_ = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    PoolWithFailover(
        const std::string & database,
        const RemoteDescription & addresses,
        const std::string & user,
        const std::string & password,
        size_t pool_size = POSTGRESQL_POOL_DEFAULT_SIZE,
        size_t pool_wait_timeout = POSTGRESQL_POOL_WAIT_TIMEOUT,
        size_t max_tries_ = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    PoolWithFailover(const PoolWithFailover & other);

    ConnectionHolderPtr get();


private:
    struct PoolHolder
    {
        String connection_string;
        PoolPtr pool;
        size_t pool_wait_timeout;

        PoolHolder(const String & connection_string_, size_t pool_size, size_t pool_wait_timeout_)
            : connection_string(connection_string_), pool(std::make_shared<Pool>(pool_size)), pool_wait_timeout(pool_wait_timeout_) {}
    };
    /// Highest priority is 0, the bigger the number in map, the less the priority
    using Replicas = std::vector<PoolHolder>;
    using ReplicasWithPriority = std::map<size_t, Replicas>;

    ReplicasWithPriority replicas_with_priority;
    size_t max_tries;
    std::mutex mutex;
};

using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;

}
