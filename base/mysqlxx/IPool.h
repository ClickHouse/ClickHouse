#pragma once

#include <cstddef>

#include <mysqlxx/Connection.h>

namespace mysqlxx
{

class IPool
{
protected:

    class ConnectionHolder
    {
    public:
        explicit ConnectionHolder(mysqlxx::Connection && connection_, std::shared_ptr<IPool> pool_)
            : connection(std::move(connection_))
            , pool(pool_)
        {}

        ~ConnectionHolder()
        {
            pool->returnConnectionToPool(std::move(connection));
        }

        mysqlxx::Connection connection;
        std::shared_ptr<IPool> pool;
    };

public:

    /// TODO: Entry should not be imitation of shared pointer, we need to return connection wrapped in unique pointer
    class Entry
    {
    public:
        Entry() = default;

        bool isNull() const
        {
            return connection_holder != nullptr;
        }

        operator mysqlxx::Connection & () & /// NOLINT
        {
            initializeMySQLThread();
            return connection_holder->connection;
        }

        operator const mysqlxx::Connection & () const & /// NOLINT
        {
            initializeMySQLThread();
            return connection_holder->connection;
        }

        const mysqlxx::Connection * operator->() const &
        {
            initializeMySQLThread();
            return &connection_holder->connection;
        }

        mysqlxx::Connection * operator->() &
        {
            initializeMySQLThread();
            return &connection_holder->connection;
        }
    private:
        friend class IPool;
        friend class Pool;

        std::shared_ptr<ConnectionHolder> connection_holder;

        static void initializeMySQLThread();
    };

    virtual ~IPool() = default;

    virtual Entry getEntry() = 0;

    virtual Entry tryGetEntry(size_t timeout_in_milliseconds) = 0;

protected:

    virtual void returnConnectionToPool(mysqlxx::Connection && connection) = 0;

};

using PoolPtr = std::shared_ptr<IPool>;

}
