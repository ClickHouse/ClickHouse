#pragma once

#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <common/BorrowedObjectPool.h>


namespace postgres
{

using ConnectionPtr = std::unique_ptr<pqxx::connection>;
using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

class ConnectionHolder
{

public:
    ConnectionHolder(const String & connection_string_, PoolPtr pool_, size_t pool_wait_timeout_);

    ConnectionHolder(const ConnectionHolder & other) = delete;

    ~ConnectionHolder();

    bool isValid() { return connection && connection->is_open(); }

    pqxx::connection & get();

private:
    String connection_string;
    PoolPtr pool;
    ConnectionPtr connection;
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}
