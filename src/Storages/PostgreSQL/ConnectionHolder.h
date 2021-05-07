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
    ConnectionHolder(PoolPtr pool_, ConnectionPtr connection_, const String & connection_string_)
        : pool(pool_), connection(std::move(connection_)), connection_string(connection_string_) {}

    ConnectionHolder(const ConnectionHolder & other) = delete;

    ~ConnectionHolder() { pool->returnObject(std::move(connection)); }

    pqxx::connection & get()
    {
        assert(connection != nullptr);
        return *connection;
    }

private:
    PoolPtr pool;
    ConnectionPtr connection;
    const String connection_string;
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}
