#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include "PostgreSQLConnectionPool.h"
#include "PostgreSQLConnection.h"


namespace DB
{

PostgreSQLConnectionPool::PostgreSQLConnectionPool(
    std::string dbname, std::string host, UInt16 port, std::string user, std::string password)
{
    address = host + ':' + std::to_string(port);
    connection_str = formatConnectionString(std::move(dbname), std::move(host), port, std::move(user), std::move(password));
    initialize();
}


PostgreSQLConnectionPool::PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other)
        : connection_str(other.connection_str)
        , address(other.address)
{
    initialize();
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


void PostgreSQLConnectionPool::initialize()
{
    /// No connection is made, just fill pool with non-connected connection objects.
    for (size_t i = 0; i < POSTGRESQL_POOL_DEFAULT_SIZE; ++i)
        pool.emplace_back(std::make_shared<PostgreSQLConnection>(connection_str, address));
}



WrappedPostgreSQLConnection PostgreSQLConnectionPool::get()
{
    std::lock_guard lock(mutex);

    for (const auto & connection : pool)
    {
        if (connection->available())
            return WrappedPostgreSQLConnection(connection);
    }

    auto connection = std::make_shared<PostgreSQLConnection>(connection_str, address);
    return WrappedPostgreSQLConnection(connection);
}


bool PostgreSQLConnectionPool::isConnected()
{
    auto connection = get();
    return connection.isConnected();
}

}

#endif
