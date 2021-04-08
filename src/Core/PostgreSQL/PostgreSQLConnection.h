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
    using ReadTransaction = pqxx::read_transaction;
    using ReplicationTransaction = pqxx::transaction<isolation_level::repeatable_read, write_policy::read_only>;
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

    pqxx::connection & getRef();

    bool isConnected() { return tryConnectIfNeeded(); }

    const String & getConnectionString() { return connection_str; }

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

    pqxx::connection & conn() const { return connection->getRef(); }

    bool isConnected() { return connection->isConnected(); }

private:
    ConnectionPtr connection;
    Pool & pool;
};


using ConnectionHolderPtr = std::shared_ptr<ConnectionHolder>;

}


#endif
