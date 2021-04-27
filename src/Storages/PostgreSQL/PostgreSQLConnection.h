#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>


namespace pqxx
{
    using ConnectionPtr = std::shared_ptr<pqxx::connection>;
}

namespace postgres
{

class Connection
{

public:
    Connection(
        const String & connection_str_,
        const String & address_);

    Connection(const Connection & other) = delete;

    pqxx::ConnectionPtr get();

    pqxx::ConnectionPtr tryGet();

    bool isConnected() { return tryConnectIfNeeded(); }

private:
    void connectIfNeeded();

    bool tryConnectIfNeeded();

    const std::string & getAddress() { return address; }

    pqxx::ConnectionPtr connection;
    std::string connection_str, address;
};

using ConnectionPtr = std::shared_ptr<Connection>;


class ConnectionHolder
{

using Pool = ConcurrentBoundedQueue<ConnectionPtr>;
static constexpr inline auto POSTGRESQL_POOL_WAIT_MS = 50;

public:
    ConnectionHolder(ConnectionPtr connection_, Pool & pool_)
        : connection(std::move(connection_))
        , pool(pool_)
    {
    }

    ConnectionHolder(const ConnectionHolder & other) = delete;

    ~ConnectionHolder() { pool.tryPush(connection, POSTGRESQL_POOL_WAIT_MS); }

    pqxx::connection & conn() const { return *connection->get(); }

    bool isConnected() { return connection->isConnected(); }

private:
    ConnectionPtr connection;
    Pool & pool;
};

using ConnectionHolderPtr = std::shared_ptr<ConnectionHolder>;

}


#endif
