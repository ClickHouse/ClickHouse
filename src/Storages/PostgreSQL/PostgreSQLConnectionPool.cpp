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
    : pool(POSTGRESQL_POOL_DEFAULT_SIZE)
{
    address = host + ':' + std::to_string(port);
    connection_str = formatConnectionString(std::move(dbname), std::move(host), port, std::move(user), std::move(password));

    /// No connection is made, just fill pool with non-connected connection objects.
    for (size_t i = 0; i < POSTGRESQL_POOL_DEFAULT_SIZE; ++i)
        pool.push(std::make_shared<PostgreSQLConnection>(connection_str, address));
}


PostgreSQLConnectionPool::PostgreSQLConnectionPool(const PostgreSQLConnectionPool & other)
        : connection_str(other.connection_str)
        , address(other.address)
        , pool(POSTGRESQL_POOL_DEFAULT_SIZE)
{
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


PostgreSQLConnection::ConnectionPtr PostgreSQLConnectionPool::get()
{
    PostgreSQLConnectionPtr connection = popConnection();
    return connection->get();
}


PostgreSQLConnection::ConnectionPtr PostgreSQLConnectionPool::tryGet()
{
    PostgreSQLConnectionPtr connection = popConnection();
    return connection->tryGet();
}


PostgreSQLConnectionPtr PostgreSQLConnectionPool::popConnection()
{
    PostgreSQLConnectionPtr connection;
    if (pool.tryPop(connection, POSTGRESQL_POOL_WAIT_POP_PUSH_MS))
        return connection;

    return std::make_shared<PostgreSQLConnection>(connection_str, address);
}


void PostgreSQLConnectionPool::put(PostgreSQLConnection::ConnectionPtr connection)
{
    pushConnection(std::make_shared<PostgreSQLConnection>(connection, connection_str, address));
}


void PostgreSQLConnectionPool::pushConnection(PostgreSQLConnectionPtr connection)
{
    pool.tryPush(connection, POSTGRESQL_POOL_WAIT_POP_PUSH_MS);
}


bool PostgreSQLConnectionPool::connected()
{
    PostgreSQLConnectionPtr connection = popConnection();
    bool result = connection->connected();
    pushConnection(connection);
    return result;
}

}

#endif
