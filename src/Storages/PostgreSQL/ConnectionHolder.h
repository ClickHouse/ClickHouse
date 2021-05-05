#pragma once

#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <common/BorrowedObjectPool.h>


namespace postgres
{

String formatConnectionString(String dbname, String host, UInt16 port, String user, String password);

using ConnectionPtr = std::unique_ptr<pqxx::connection>;
using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

class ConnectionHolder
{

public:
    ConnectionHolder(const String & connection_string_, PoolPtr pool_, size_t pool_wait_timeout_);

    ConnectionHolder(const ConnectionHolder & other) = delete;

    ~ConnectionHolder();

    /// Will throw if error is not pqxx::broken_connection.
    bool isConnected();
    /// Throw on no connection.
    pqxx::connection & get();

private:
    String connection_string;
    PoolPtr pool;
    ConnectionPtr connection;
    size_t pool_wait_timeout;
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}
