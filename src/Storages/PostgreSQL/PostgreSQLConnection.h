#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx>


namespace DB
{

/// Tiny connection class to make it more convenient to use.
/// Connection is not made until actually used.
class PostgreSQLConnection
{
    using ConnectionPtr = std::shared_ptr<pqxx::connection>;

public:
    PostgreSQLConnection(const std::string & connection_str_) : connection_str(connection_str_) {}
    PostgreSQLConnection(const PostgreSQLConnection &) = delete;
    PostgreSQLConnection operator =(const PostgreSQLConnection &) = delete;

    ConnectionPtr conn()
    {
        checkUpdateConnection();
        return connection;
    }

    std::string & conn_str() { return connection_str; }

private:
    ConnectionPtr connection;
    std::string connection_str;

    void checkUpdateConnection()
    {
        if (!connection || !connection->is_open())
            connection = std::make_unique<pqxx::connection>(connection_str);
    }
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;

}

#endif
