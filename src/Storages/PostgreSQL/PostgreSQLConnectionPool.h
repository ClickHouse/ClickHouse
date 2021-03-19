#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"


namespace DB
{

class PostgreSQLReplicaConnection;


/// Connection pool of size POSTGRESQL_POOL_DEFAULT_SIZE = 16.
/// If it was not possible to fetch connection within a timeout, a new connection is made.
/// If it was not possible to put connection back into pool within a timeout, it is closed.
class PostgreSQLConnectionPool
{

friend class PostgreSQLReplicaConnection;

public:

    PostgreSQLConnectionPool(std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other);

    PostgreSQLConnectionPool operator =(const PostgreSQLConnectionPool &) = delete;

    PostgreSQLConnectionHolderPtr get();

private:
    static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;
    static constexpr inline auto POSTGRESQL_POOL_WAIT_MS = 50;

    using Pool = ConcurrentBoundedQueue<PostgreSQLConnectionPtr>;
    using PoolPtr = std::shared_ptr<Pool>;

    static std::string formatConnectionString(
        std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    void initialize();

    PoolPtr pool;
    std::string connection_str, address;
};

using PostgreSQLConnectionPoolPtr = std::shared_ptr<PostgreSQLConnectionPool>;

}

#endif
