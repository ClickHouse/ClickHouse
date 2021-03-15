#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"
#include <common/logger_useful.h>
#include <IO/Operators.h>


namespace DB
{

PostgreSQLConnection::PostgreSQLConnection(
        const String & connection_str_,
        const String & address_)
    : connection_str(connection_str_)
    , address(address_)
{
}


PostgreSQLConnection::PostgreSQLConnection(
        ConnectionPtr connection_,
        const String & connection_str_,
        const String & address_)
    : connection(std::move(connection_))
    , connection_str(connection_str_)
    , address(address_)
{
}


PostgreSQLConnection::PostgreSQLConnection(const PostgreSQLConnection & other)
        : connection_str(other.connection_str)
        , address(other.address)
{
}


PostgreSQLConnection::ConnectionPtr PostgreSQLConnection::get()
{
    connect();
    return connection;
}


PostgreSQLConnection::ConnectionPtr PostgreSQLConnection::tryGet()
{
    if (tryConnect())
        return connection;
    return nullptr;
}


void PostgreSQLConnection::connect()
{
    if (!connection || !connection->is_open())
        connection = std::make_shared<pqxx::connection>(connection_str);
}


bool PostgreSQLConnection::tryConnect()
{
    try
    {
        connect();
    }
    catch (const pqxx::broken_connection & pqxx_error)
    {
        LOG_ERROR(
            &Poco::Logger::get("PostgreSQLConnection"),
            "Unable to setup connection to {}, reason: {}",
            getAddress(), pqxx_error.what());
        return false;
    }
    catch (...)
    {
        throw;
    }

    return true;
}

}

#endif
