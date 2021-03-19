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


PostgreSQLConnection::ConnectionPtr PostgreSQLConnection::get()
{
    connectIfNeeded();
    return connection;
}


PostgreSQLConnection::ConnectionPtr PostgreSQLConnection::tryGet()
{
    if (tryConnectIfNeeded())
        return connection;
    return nullptr;
}


void PostgreSQLConnection::connectIfNeeded()
{
    if (!connection || !connection->is_open())
    {
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLConnection"), "New connection to {}", getAddress());
        connection = std::make_shared<pqxx::connection>(connection_str);
    }
}


bool PostgreSQLConnection::tryConnectIfNeeded()
{
    try
    {
        connectIfNeeded();
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
