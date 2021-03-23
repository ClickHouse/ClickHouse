#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>


namespace DB
{

class PostgreSQLConnection
{

using ConnectionPtr = std::shared_ptr<pqxx::connection>;

public:
    PostgreSQLConnection(
        const String & connection_str_,
        const String & address_);

    PostgreSQLConnection(const PostgreSQLConnection & other) = delete;

    ConnectionPtr get();

    ConnectionPtr tryGet();

    bool isConnected() { return tryConnectIfNeeded(); }

private:
    void connectIfNeeded();

    bool tryConnectIfNeeded();

    const std::string & getAddress() { return address; }

    ConnectionPtr connection;
    std::string connection_str, address;
};

using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


class PostgreSQLConnectionHolder
{

using Pool = ConcurrentBoundedQueue<PostgreSQLConnectionPtr>;
static constexpr inline auto POSTGRESQL_POOL_WAIT_MS = 50;

public:
    PostgreSQLConnectionHolder(PostgreSQLConnectionPtr connection_, Pool & pool_)
        : connection(std::move(connection_))
        , pool(pool_)
    {
    }

    PostgreSQLConnectionHolder(const PostgreSQLConnectionHolder & other) = delete;

    ~PostgreSQLConnectionHolder() { pool.tryPush(connection, POSTGRESQL_POOL_WAIT_MS); }

    pqxx::connection & conn() const { return *connection->get(); }

    bool isConnected() { return connection->isConnected(); }

private:
    PostgreSQLConnectionPtr connection;
    Pool & pool;
};

using PostgreSQLConnectionHolderPtr = std::shared_ptr<PostgreSQLConnectionHolder>;

}


#endif
