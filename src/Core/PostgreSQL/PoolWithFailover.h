#pragma once

#include "config_core.h"

#if USE_LIBPQXX


#include "ConnectionHolder.h"
#include <mutex>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/logger_useful.h>
#include <Storages/ExternalDataSourceConfiguration.h>


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
        const DB::ExternalDataSourcesConfigurationByPriority & configurations_by_priority,
        size_t pool_size = POSTGRESQL_POOL_DEFAULT_SIZE,
        size_t pool_wait_timeout = POSTGRESQL_POOL_WAIT_TIMEOUT,
        size_t max_tries_ = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    PoolWithFailover(
        const DB::StoragePostgreSQLConfiguration & configuration,
        size_t pool_size = POSTGRESQL_POOL_DEFAULT_SIZE,
        size_t pool_wait_timeout = POSTGRESQL_POOL_WAIT_TIMEOUT,
        size_t max_tries_ = POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

    PoolWithFailover(const PoolWithFailover & other) = delete;

    ConnectionHolderPtr get();

private:
    struct PoolHolder
    {
        String connection_string;
        PoolPtr pool;
        String name_for_log;

        PoolHolder(const String & connection_string_, size_t pool_size, const String & name_for_log_)
            : connection_string(connection_string_), pool(std::make_shared<Pool>(pool_size)), name_for_log(name_for_log_) {}
    };

    /// Highest priority is 0, the bigger the number in map, the less the priority
    using Replicas = std::vector<PoolHolder>;
    using ReplicasWithPriority = std::map<size_t, Replicas>;

    ReplicasWithPriority replicas_with_priority;
    size_t pool_wait_timeout;
    size_t max_tries;
    std::mutex mutex;
    Poco::Logger * log = &Poco::Logger::get("PostgreSQLConnectionPool");
};

using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;

}

#endif
