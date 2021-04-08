#pragma once

#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "PostgreSQLConnectionPool.h"
#include <mutex>


namespace DB
{

class PostgreSQLReplicaConnection
{

public:
    static constexpr inline auto POSTGRESQL_CONNECTION_DEFAULT_RETRIES_NUM = 5;

    PostgreSQLReplicaConnection(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const size_t num_retries_ = POSTGRESQL_CONNECTION_DEFAULT_RETRIES_NUM);

    PostgreSQLReplicaConnection(const PostgreSQLReplicaConnection & other);

    PostgreSQLConnectionHolderPtr get();


private:
    /// Highest priority is 0, the bigger the number in map, the less the priority
    using ReplicasByPriority = std::map<size_t, PostgreSQLConnectionPoolPtr>;

    ReplicasByPriority replicas;
    size_t num_retries;
    std::mutex mutex;
};

using PostgreSQLReplicaConnectionPtr = std::shared_ptr<PostgreSQLReplicaConnection>;

}
