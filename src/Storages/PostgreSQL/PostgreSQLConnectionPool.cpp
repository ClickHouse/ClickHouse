#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include "PostgreSQLConnectionPool.h"
#include "PostgreSQLConnection.h"
#include <common/logger_useful.h>


namespace DB
{

PostgreSQLConnectionPool::PostgreSQLConnectionPool(
    std::string dbname, std::string host, UInt16 port, std::string user, std::string password)
    : pool(std::make_shared<Pool>(POSTGRESQL_POOL_DEFAULT_SIZE))
{
    address = host + ':' + std::to_string(port);
    connection_str = formatConnectionString(std::move(dbname), std::move(host), port, std::move(user), std::move(password));
    initialize();
}


PostgreSQLConnectionPool::PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other)
        : pool(std::make_shared<Pool>(POSTGRESQL_POOL_DEFAULT_SIZE))
        , connection_str(other.connection_str)
        , address(other.address)
{
    initialize();
}


void PostgreSQLConnectionPool::initialize()
{
    /// No connection is made, just fill pool with non-connected connection objects.
    for (size_t i = 0; i < POSTGRESQL_POOL_DEFAULT_SIZE; ++i)
        pool->push(std::make_shared<PostgreSQLConnection>(connection_str, address));
}


std::string PostgreSQLConnectionPool::formatConnectionString(
    std::string dbname, std::string host, UInt16 port, std::string user, std::string password)
{
    WriteBufferFromOwnString out;
    out << "dbname=" << quote << dbname
        << " host=" << quote << host
        << " port=" << port
        << " user=" << quote << user
        << " password=" << quote << password;
    return out.str();
}


PostgreSQLConnectionHolderPtr PostgreSQLConnectionPool::get()
{
    PostgreSQLConnectionPtr connection;
    if (pool->tryPop(connection, POSTGRESQL_POOL_WAIT_MS))
    {
        return std::make_shared<PostgreSQLConnectionHolder>(connection, *pool);
    }

    connection = std::make_shared<PostgreSQLConnection>(connection_str, address);
    return std::make_shared<PostgreSQLConnectionHolder>(connection, *pool);
}

}

#endif
