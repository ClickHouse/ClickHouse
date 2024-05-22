#pragma once

#include "config.h"

#if USE_LIBPQXX


#include "ConnectionHolder.h"
#include <mutex>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StoragePostgreSQL.h>


static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;
static constexpr inline auto POSTGRESQL_POOL_WAIT_TIMEOUT = 5000;
static constexpr inline auto POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES = 2;

namespace postgres
{

class PoolWithFailover
{

using RemoteDescription = std::vector<std::pair<String, uint16_t>>;

public:
    PoolWithFailover(
        const DB::ExternalDataSourcesConfigurationByPriority & configurations_by_priority,
        size_t pool_size,
        size_t pool_wait_timeout,
        size_t max_tries_,
        bool auto_close_connection_);

    explicit PoolWithFailover(
        const DB::StoragePostgreSQL::Configuration & configuration,
        size_t pool_size,
        size_t pool_wait_timeout,
        size_t max_tries_,
        bool auto_close_connection_);

    PoolWithFailover(const PoolWithFailover & other) = delete;

    ConnectionHolderPtr get();

private:
    struct PoolHolder
    {
        ConnectionInfo connection_info;
        PoolPtr pool;

        PoolHolder(const ConnectionInfo & connection_info_, size_t pool_size)
            : connection_info(connection_info_), pool(std::make_shared<Pool>(pool_size)) {}
    };

    /// Highest priority is 0, the bigger the number in map, the less the priority
    using Replicas = std::vector<PoolHolder>;
    using ReplicasWithPriority = std::map<size_t, Replicas>;

    ReplicasWithPriority replicas_with_priority;
    size_t pool_wait_timeout;
    size_t max_tries;
    bool auto_close_connection;
    std::mutex mutex;
    LoggerPtr log = getLogger("PostgreSQLConnectionPool");
};

using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;

}

#endif
