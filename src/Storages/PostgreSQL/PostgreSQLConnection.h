#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <common/logger_useful.h>


namespace DB
{

class WrappedPostgreSQLConnection;

class PostgreSQLConnection
{

friend class WrappedPostgreSQLConnection;

using ConnectionPtr = std::shared_ptr<pqxx::connection>;

public:
    PostgreSQLConnection(
        const String & connection_str_,
        const String & address_);

    PostgreSQLConnection(const PostgreSQLConnection & other) = delete;

    ConnectionPtr get();

    ConnectionPtr tryGet();

    bool isConnected() { return tryConnectIfNeeded(); }

    int32_t isAvailable() { return !locked.load(); }

protected:
    void lock() { locked.store(true); }

    void unlock() { locked.store(false); }

private:
    void connectIfNeeded();

    bool tryConnectIfNeeded();

    const std::string & getAddress() { return address; }

    ConnectionPtr connection;
    std::string connection_str, address;
    std::atomic<bool> locked{false};
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


class WrappedPostgreSQLConnection
{

public:
    WrappedPostgreSQLConnection(PostgreSQLConnectionPtr connection_) : connection(connection_) { connection->lock(); }

    WrappedPostgreSQLConnection(const WrappedPostgreSQLConnection & other) = delete;

    ~WrappedPostgreSQLConnection() { connection->unlock(); }

    pqxx::connection & conn() const { return *connection->get(); }

    bool isConnected() { return connection->isConnected(); }

private:
    PostgreSQLConnectionPtr connection;
};

using WrappedPostgreSQLConnectionPtr = std::shared_ptr<WrappedPostgreSQLConnection>;

}


#endif
