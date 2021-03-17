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

using ConnectionPtr = std::shared_ptr<pqxx::connection>;

public:
    PostgreSQLConnection(
        const String & connection_str_,
        const String & address_);

    PostgreSQLConnection(const PostgreSQLConnection & other);

    ConnectionPtr get();

    ConnectionPtr tryGet();

    bool isConnected() { return tryConnectIfNeeded(); }

    bool available() { return ref_count.load() == 0; }

    void incrementRef() { ref_count++; }

    void decrementRef() { ref_count--; }

private:
    void connectIfNeeded();

    bool tryConnectIfNeeded();

    const std::string & getAddress() { return address; }

    ConnectionPtr connection;
    std::string connection_str, address;
    std::atomic<uint8_t> ref_count{0};
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


class WrappedPostgreSQLConnection
{

public:
    WrappedPostgreSQLConnection(PostgreSQLConnectionPtr connection_) : connection(connection_) { connection->incrementRef(); }

    WrappedPostgreSQLConnection(const WrappedPostgreSQLConnection & other) : connection(other.connection) {}

    ~WrappedPostgreSQLConnection() { connection->decrementRef(); }

    pqxx::connection & operator*() const { return *connection->get(); }

    pqxx::connection * operator->() const { return connection->get().get(); }

    bool isConnected() { return connection->isConnected(); }

private:
    PostgreSQLConnectionPtr connection;
};

}


#endif
