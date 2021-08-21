#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

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
    ConnectionHolder(PoolPtr pool_, ConnectionPtr connection_) : pool(pool_), connection(std::move(connection_)) {}

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
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}

#endif
