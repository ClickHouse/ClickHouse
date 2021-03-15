#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Common/ConcurrentBoundedQueue.h>
#include "PostgreSQLConnection.h"
#include <pqxx/pqxx> // Y_IGNORE

namespace DB
{

class PostgreSQLConnectionPool
{

public:

    PostgreSQLConnectionPool(std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other);

    PostgreSQLConnectionPool operator =(const PostgreSQLConnectionPool &) = delete;

    /// Will throw if unable to setup connection.
    PostgreSQLConnection::ConnectionPtr get();

    /// Will return nullptr if connection was not established.
    PostgreSQLConnection::ConnectionPtr tryGet();

    void put(PostgreSQLConnection::ConnectionPtr connection);

    bool connected();

    std::string & conn_str() { return connection_str; }

private:
    static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;
    static constexpr inline auto POSTGRESQL_POOL_WAIT_POP_PUSH_MS = 100;

    using Pool = ConcurrentBoundedQueue<PostgreSQLConnectionPtr>;

    static std::string formatConnectionString(
        std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    /// Try get connection from connection pool with timeout.
    /// If pool is empty after timeout, make a new connection.
    PostgreSQLConnectionPtr popConnection();

    /// Put connection object back into pool.
    void pushConnection(PostgreSQLConnectionPtr connection);

    std::string connection_str, address;
    Pool pool;
};

using PostgreSQLConnectionPoolPtr = std::shared_ptr<PostgreSQLConnectionPool>;

}

#endif
