#include "PostgreSQLConnection.h"

#if USE_LIBPQXX
#include <IO/WriteBufferFromString.h>
#include <common/logger_useful.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int POSTGRESQL_CONNECTION_FAILURE;
}
}

namespace postgres
{

Connection::Connection(
        const String & connection_str_,
        const String & address_)
    : connection_str(connection_str_)
    , address(address_)
{
}


pqxx::ConnectionPtr Connection::get()
{
    connectIfNeeded();
    return connection;
}


pqxx::connection & Connection::getRef()
{
    if (tryConnectIfNeeded())
        return *connection;

    throw DB::Exception(DB::ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "Connection failure");
}


pqxx::ConnectionPtr Connection::tryGet()
{
    if (tryConnectIfNeeded())
        return connection;
    return nullptr;
}


void Connection::connectIfNeeded()
{
    if (!connection || !connection->is_open())
    {
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLConnection"), "New connection to {}", getAddress());
        connection = std::make_shared<pqxx::connection>(connection_str);
    }
}


bool Connection::tryConnectIfNeeded()
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
