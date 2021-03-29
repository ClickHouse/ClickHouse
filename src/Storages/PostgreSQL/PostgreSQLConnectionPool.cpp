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
        std::string dbname,
        std::string host,
        UInt16 port,
        std::string user,
        std::string password,
        size_t pool_size_,
        int64_t pool_wait_timeout_)
        : pool(std::make_shared<Pool>(pool_size_))
        , pool_size(pool_size_)
        , pool_wait_timeout(pool_wait_timeout_)
        , block_on_empty_pool(pool_wait_timeout == -1)
{
    LOG_INFO(
        &Poco::Logger::get("PostgreSQLConnectionPool"),
        "New connection pool. Size: {}, blocks on empty pool: {}",
        pool_size, block_on_empty_pool);

    address = host + ':' + std::to_string(port);
    connection_str = formatConnectionString(std::move(dbname), std::move(host), port, std::move(user), std::move(password));
    initialize();
}


PostgreSQLConnectionPool::PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other)
        : pool(std::make_shared<Pool>(other.pool_size))
        , connection_str(other.connection_str)
        , address(other.address)
        , pool_size(other.pool_size)
        , pool_wait_timeout(other.pool_wait_timeout)
        , block_on_empty_pool(other.block_on_empty_pool)
{
    initialize();
}


void PostgreSQLConnectionPool::initialize()
{
    /// No connection is made, just fill pool with non-connected connection objects.
    for (size_t i = 0; i < pool_size; ++i)
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

    /// Always blocks by default.
    if (block_on_empty_pool)
    {
        /// pop to ConcurrentBoundedQueue will block until it is non-empty.
        pool->pop(connection);
        return std::make_shared<PostgreSQLConnectionHolder>(connection, *pool);
    }

    if (pool->tryPop(connection, pool_wait_timeout))
    {
        return std::make_shared<PostgreSQLConnectionHolder>(connection, *pool);
    }

    connection = std::make_shared<PostgreSQLConnection>(connection_str, address);
    return std::make_shared<PostgreSQLConnectionHolder>(connection, *pool);
}

}

#endif
