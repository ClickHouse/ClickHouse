#pragma once

#include "PostgreSQLConnection.h"
#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>

namespace DB
{

class PostgreSQLReplicaConnection
{

public:
    static constexpr inline auto POSTGRESQL_CONNECTION_DEFAULT_RETRIES_NUM = 5;

    PostgreSQLReplicaConnection(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_name,
        const size_t num_retries = POSTGRESQL_CONNECTION_DEFAULT_RETRIES_NUM);

    PostgreSQLReplicaConnection(const PostgreSQLReplicaConnection & other);

    PostgreSQLConnection::ConnectionPtr get();


private:
    using ReplicasByPriority = std::map<size_t, PostgreSQLConnectionPtr>;

    Poco::Logger * log;
    ReplicasByPriority replicas;
    size_t num_retries;
};

using PostgreSQLReplicaConnectionPtr = std::shared_ptr<PostgreSQLReplicaConnection>;

}
