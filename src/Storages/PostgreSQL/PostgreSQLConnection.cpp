#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Storages/PostgreSQL/PostgreSQLConnection.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

PostgreSQLConnection::ConnectionPtr PostgreSQLConnection::conn()
{
    checkUpdateConnection();
    return connection;
}

void PostgreSQLConnection::checkUpdateConnection()
{
    if (!connection || !connection->is_open())
        connection = std::make_unique<pqxx::connection>(connection_str);
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
