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

    PostgreSQLConnection(std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    PostgreSQLConnection(const PostgreSQLConnection & other);

    PostgreSQLConnection operator =(const PostgreSQLConnection &) = delete;

    bool tryConnect();

    ConnectionPtr conn();

    const std::string & getAddress() { return address; }

    std::string & conn_str() { return connection_str; }

private:
    void connect();

    static std::string formatConnectionString(
        std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    ConnectionPtr connection;
    std::string connection_str, address;
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;

}

#endif
