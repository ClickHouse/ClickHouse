#pragma once

#include "config_core.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include <base/BorrowedObjectPool.h>
#include "Connection.h"


namespace postgres
{

using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

class ConnectionHolder
{

public:
    ConnectionHolder(PoolPtr pool_, ConnectionPtr connection_) : pool(pool_), connection(std::move(connection_)) {}

    ConnectionHolder(const ConnectionHolder & other) = delete;

    ~ConnectionHolder() { pool->returnObject(std::move(connection)); }

    pqxx::connection & get()
    {
        return connection->getRef();
    }

    void update()
    {
        connection->updateConnection();
    }

private:
    PoolPtr pool;
    ConnectionPtr connection;
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}

#endif
