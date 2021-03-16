#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Common/ConcurrentBoundedQueue.h>
#include "PostgreSQLConnection.h"


namespace DB
{
class PostgreSQLReplicaConnection;

class PostgreSQLConnectionPool
{
friend class PostgreSQLReplicaConnection;

public:

    PostgreSQLConnectionPool(std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other);

    PostgreSQLConnectionPool operator =(const PostgreSQLConnectionPool &) = delete;

    WrappedPostgreSQLConnection get();

protected:
    bool isConnected();

private:
    static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;
    static constexpr inline auto POSTGRESQL_POOL_WAIT_POP_PUSH_MS = 100;

    using Pool = std::vector<PostgreSQLConnectionPtr>;

    static std::string formatConnectionString(
        std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    void initialize();

    std::string connection_str, address;
    Pool pool;
    std::mutex mutex;
};

using PostgreSQLConnectionPoolPtr = std::shared_ptr<PostgreSQLConnectionPool>;

}

#endif
