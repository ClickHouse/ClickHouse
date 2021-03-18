#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Storages/PostgreSQL/PostgreSQLConnection.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <common/logger_useful.h>


namespace DB
{

PostgreSQLConnection::PostgreSQLConnection(std::string dbname, std::string host, UInt16 port, std::string user, std::string password)
{
    address = host + ':' + std::to_string(port);
    connection_str = formatConnectionString(std::move(dbname), std::move(host), port, std::move(user), std::move(password));
}


PostgreSQLConnection::PostgreSQLConnection(const PostgreSQLConnection & other)
        : connection_str(other.connection_str)
        , address(other.address)
{
}


PostgreSQLConnection::ConnectionPtr PostgreSQLConnection::conn()
{
    connect();
    return connection;
}


void PostgreSQLConnection::connect()
{
    if (!connection || !connection->is_open())
        connection = std::make_unique<pqxx::connection>(connection_str);
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


std::string PostgreSQLConnection::formatConnectionString(
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

}

#endif
