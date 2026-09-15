#pragma once

#include "config.h"
#include <memory>

#if USE_LIBPQXX


#include <Core/PostgreSQL/ConnectionHolder.h>
#include <mutex>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/StoragePostgreSQL.h>


static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;
static constexpr inline auto POSTGRESQL_POOL_WAIT_TIMEOUT = 5000;

namespace postgres
{

class PoolWithFailover
{
public:
    using ReplicasConfigurationByPriority = std::map<size_t, std::vector<DB::StoragePostgreSQL::Configuration>>;
    using RemoteDescription = std::vector<std::pair<String, uint16_t>>;

    PoolWithFailover(
        const ReplicasConfigurationByPriority & configurations_by_priority,
        size_t pool_size,
        size_t pool_wait_timeout,
        size_t max_tries_,
        bool auto_close_connection_,
        size_t connection_attempt_timeout_,
        bool bg_reconnect_ = false);

    explicit PoolWithFailover(
        const DB::StoragePostgreSQL::Configuration & configuration,
        size_t pool_size,
        size_t pool_wait_timeout,
        size_t max_tries_,
        bool auto_close_connection_,
        size_t connection_attempt_timeout_,
        bool bg_reconnect_ = false);

    PoolWithFailover(const PoolWithFailover & other) = delete;

    ConnectionHolderPtr get();

private:
    struct PoolHolder
    {
        ConnectionInfo connection_info;
        PoolPtr pool;
        /// Pool is online.
        std::atomic<bool> online{true};

        PoolHolder(const ConnectionInfo & connection_info_, size_t pool_size)
            : connection_info(connection_info_), pool(std::make_shared<Pool>(pool_size)) {}
    };

    /// Highest priority is 0, the bigger the number in map, the less the priority
    using PoolHolderPtr = std::shared_ptr<PoolHolder>;
    using Replicas = std::vector<PoolHolderPtr>;
    using ReplicasWithPriority = std::map<size_t, Replicas>;

    static auto connectionReestablisher(std::weak_ptr<PoolHolder> pool, size_t pool_wait_timeout);

    ReplicasWithPriority replicas_with_priority;
    size_t pool_wait_timeout;
    size_t max_tries;
    bool auto_close_connection;
    bool bg_reconnect;
    std::mutex mutex;
    LoggerPtr log = getLogger("PostgreSQLConnectionPool");
};

using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;

}

#endif
