#include "PostgreSQLConnection.h"

#if USE_LIBPQXX
#include <IO/Operators.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int POSTGRESQL_CONNECTION_FAILURE;
}
}

namespace postgres
{

ConnectionInfo formatConnectionString(
    std::string dbname, std::string host, UInt16 port, std::string user, std::string password)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password;
    return std::make_pair(out.str(), host + ':' + DB::toString(port));
}


ConnectionPtr createReplicationConnection(const ConnectionInfo & connection_info)
{
    auto new_connection_info = std::make_pair(
            fmt::format("{} replication=database", connection_info.first),
            connection_info.second);

    auto connection = std::make_shared<postgres::Connection>(new_connection_info);
    connection->get()->set_variable("default_transaction_isolation", "'repeatable read'");

    return connection;
}


Connection::Connection(const ConnectionInfo & connection_info_)
    : connection_info(connection_info_)
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
        connection = std::make_shared<pqxx::connection>(connection_info.first);
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLConnection"), "New connection to {}", connection_info.second);
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
            "Unable to setup connection to {}, reason: {}", connection_info.second, pqxx_error.what());
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
