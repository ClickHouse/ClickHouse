#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <IO/WriteBufferFromString.h>


namespace pqxx
{
    using ConnectionPtr = std::shared_ptr<pqxx::connection>;
    using ReadTransaction = pqxx::read_transaction;
    using ReplicationTransaction = pqxx::transaction<isolation_level::repeatable_read, write_policy::read_only>;
}

namespace postgres
{

class Connection;
using ConnectionPtr = std::shared_ptr<Connection>;


/// Connection string and address without login/password (for error logs)
using ConnectionInfo = std::pair<std::string, std::string>;

ConnectionInfo formatConnectionString(
    std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

ConnectionPtr createReplicationConnection(const ConnectionInfo & connection_info);


template <typename T>
class Transaction
{
public:
    Transaction(pqxx::connection & connection) : transaction(connection) {}

    ~Transaction() { transaction.commit(); }

    T & getRef() { return transaction; }

    void exec(const String & query) { transaction.exec(query); }

private:
    T transaction;
};


class Connection
{
public:
    Connection(const ConnectionInfo & connection_info_);

    Connection(const Connection & other) = delete;

    pqxx::ConnectionPtr get();

    pqxx::ConnectionPtr tryGet();

    pqxx::connection & getRef();

    bool isConnected() { return tryConnectIfNeeded(); }

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

private:
    void connectIfNeeded();

    bool tryConnectIfNeeded();

    pqxx::ConnectionPtr connection;
    ConnectionInfo connection_info;
};


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
