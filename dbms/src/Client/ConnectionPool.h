#pragma once

#include <Common/PoolBase.h>

#include <Client/Connection.h>


namespace DB
{

/** Interface for connection pools.
  *
  * Usage (using the usual `ConnectionPool` example)
  * ConnectionPool pool(...);
  *
  *    void thread()
  *    {
  *          auto connection = pool.get();
  *        connection->sendQuery("SELECT 'Hello, world!' AS world");
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
    virtual Entry get(const Settings * settings = nullptr, bool force_connected = true) = 0;
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
            const String & host_, UInt16 port_,
            const String & default_database_,
            const String & user_, const String & password_,
            const String & client_name_ = "client",
            Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
            Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
            Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
            Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0))
       : Base(max_connections_, &Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")")),
        host(host_), port(port_), default_database(default_database_),
        user(user_), password(password_), resolved_address(host_, port_),
        client_name(client_name_), compression(compression_),
        connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_)
    {
    }

    ConnectionPool(unsigned max_connections_,
            const String & host_, UInt16 port_, const Poco::Net::SocketAddress & resolved_address_,
            const String & default_database_,
            const String & user_, const String & password_,
            const String & client_name_ = "client",
            Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
            Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
            Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
            Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0))
        : Base(max_connections_, &Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")")),
        host(host_), port(port_), default_database(default_database_),
        user(user_), password(password_), resolved_address(resolved_address_),
        client_name(client_name_), compression(compression_),
        connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_)
    {
    }

    Entry get(const Settings * settings = nullptr, bool force_connected = true) override
    {
        Entry entry;
        if (settings)
            entry = Base::get(settings->queue_max_wait_ms.totalMilliseconds());
        else
            entry = Base::get(-1);

        if (force_connected)
            entry->forceConnected();

        return entry;
    }

    const std::string & getHost() const
    {
        return host;
    }

protected:
    /** Creates a new object to put in the pool. */
    ConnectionPtr allocObject() override
    {
        return std::make_shared<Connection>(
            host, port, resolved_address,
            default_database, user, password,
            client_name, compression,
            connect_timeout, receive_timeout, send_timeout);
    }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;

    /** The address can be resolved in advance and passed to the constructor. Then `host` and `port` fields are meaningful only for logging.
      * Otherwise, address is resolved in constructor. That is, DNS balancing is not supported.
      */
    Poco::Net::SocketAddress resolved_address;

    String client_name;
    Protocol::Compression::Enum compression;        /// Whether to compress data when interacting with the server.

    Poco::Timespan connect_timeout;
    Poco::Timespan receive_timeout;
    Poco::Timespan send_timeout;
};

}
