#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include <base/BorrowedObjectPool.h>
#include <Core/PostgreSQL/Connection.h>


namespace postgres
{

using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

class ConnectionHolder
{

public:
    ConnectionHolder(PoolPtr pool_, ConnectionPtr connection_, bool auto_close_)
        : pool(pool_)
        , connection(std::move(connection_))
        , auto_close(auto_close_)
    {}

    ConnectionHolder(const ConnectionHolder & other) = delete;

    void setBroken() { is_broken = true; }

    ~ConnectionHolder()
    {
        if (auto_close)
        {
            connection.reset();
        }
        else if (is_broken)
        {
            try
            {
                connection->getRef().reset();
            }
            catch (...)
            {
                connection.reset();
            }
        }
        pool->returnObject(std::move(connection));
    }

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
    bool auto_close;
    bool is_broken = false;
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}

#endif
