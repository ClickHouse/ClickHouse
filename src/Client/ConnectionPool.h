#pragma once

#include <Common/PoolBase.h>

#include <Client/Connection.h>
#include <IO/ConnectionTimeouts.h>

namespace DB
{

/** Interface for connection pools.
  *
  * Usage (using the usual `ConnectionPool` example)
  * ConnectionPool pool(...);
  *
  *    void thread()
  *    {
  *        auto connection = pool.get();
  *        connection->sendQuery(...);
  *    }
  */

class IConnectionPool : private boost::noncopyable
{
public:
    using Entry = PoolBase<Connection>::Entry;

public:
    virtual ~IConnectionPool() {}

    /// Selects the connection to work.
    /// If force_connected is false, the client must manually ensure that returned connection is good.
    virtual Entry get(const ConnectionTimeouts & timeouts,
                      const Settings * settings = nullptr,
                      bool force_connected = true) = 0;

    virtual Int64 getPriority() const { return 1; }
};

using ConnectionPoolPtr = std::shared_ptr<IConnectionPool>;
using ConnectionPoolPtrs = std::vector<ConnectionPoolPtr>;

/** A common connection pool, without fault tolerance.
  */
class ConnectionPool : public IConnectionPool, private PoolBase<Connection>
{
public:
    using Entry = IConnectionPool::Entry;
    using Base = PoolBase<Connection>;

    ConnectionPool(unsigned max_connections_,
            const String & host_,
            UInt16 port_,
            const String & default_database_,
            const String & user_,
            const String & password_,
            const String & client_name_ = "client",
            Protocol::Compression compression_ = Protocol::Compression::Enable,
            Protocol::Secure secure_ = Protocol::Secure::Disable,
            Int64 priority_ = 1)
       : Base(max_connections_,
        &Poco::Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")")),
        host(host_),
        port(port_),
        default_database(default_database_),
        user(user_),
        password(password_),
        client_name(client_name_),
        compression(compression_),
        secure(secure_),
        priority(priority_)
    {
    }

    Entry get(const ConnectionTimeouts & timeouts,
              const Settings * settings = nullptr,
              bool force_connected = true) override
    {
        Entry entry;
        if (settings)
            entry = Base::get(settings->connection_pool_max_wait_ms.totalMilliseconds());
        else
            entry = Base::get(-1);

        if (force_connected)
            entry->forceConnected(timeouts);

        return entry;
    }

    const std::string & getHost() const
    {
        return host;
    }
    std::string getDescription() const
    {
        return host + ":" + toString(port);
    }

    Int64 getPriority() const override
    {
        return priority;
    }

protected:
    /** Creates a new object to put in the pool. */
    ConnectionPtr allocObject() override
    {
        return std::make_shared<Connection>(
            host, port,
            default_database, user, password,
            client_name, compression, secure);
    }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;

    String client_name;
    Protocol::Compression compression; /// Whether to compress data when interacting with the server.
    Protocol::Secure secure;           /// Whether to encrypt data when interacting with the server.
    Int64 priority;                    /// priority from <remote_servers>

};

}
