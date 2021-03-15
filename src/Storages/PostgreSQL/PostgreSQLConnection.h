#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>


namespace DB
{


class PostgreSQLConnection
{

public:
    using ConnectionPtr = std::shared_ptr<pqxx::connection>;

    PostgreSQLConnection(
        const String & connection_str_,
        const String & address_);

    PostgreSQLConnection(
        ConnectionPtr connection_,
        const String & connection_str_,
        const String & address_);

    PostgreSQLConnection(const PostgreSQLConnection & other);

    const std::string & getAddress() { return address; }

    ConnectionPtr get();

    ConnectionPtr tryGet();

    bool connected() { return tryConnect(); }

private:
    void connect();

    bool tryConnect();

    ConnectionPtr connection;
    std::string connection_str;
    std::string address;
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;

}


#endif
