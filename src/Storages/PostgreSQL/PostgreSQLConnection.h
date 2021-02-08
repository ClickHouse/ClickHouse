#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>


namespace DB
{

/// Tiny connection class to make it more convenient to use.
/// Connection is not made until actually used.
class PostgreSQLConnection
{

public:
    using ConnectionPtr = std::shared_ptr<pqxx::connection>;

    PostgreSQLConnection(std::string dbname, std::string host, UInt16 port, std::string user, std::string password)
        : connection_str(formatConnectionString(std::move(dbname), std::move(host), port, std::move(user), std::move(password))) {}

    PostgreSQLConnection(const std::string & connection_str_) : connection_str(connection_str_) {}

    PostgreSQLConnection(const PostgreSQLConnection &) = delete;
    PostgreSQLConnection operator =(const PostgreSQLConnection &) = delete;

    ConnectionPtr conn();

    std::string & conn_str() { return connection_str; }

private:
    ConnectionPtr connection;
    std::string connection_str;

    static std::string formatConnectionString(
        std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    void checkUpdateConnection();
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;

}

#endif
