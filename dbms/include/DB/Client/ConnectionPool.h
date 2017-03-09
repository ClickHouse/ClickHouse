#pragma once

#include <DB/Common/PoolBase.h>

#include <DB/Client/Connection.h>


namespace DB
{

/** Interface for connection pools.
  *
  * Usage (using the usual `ConnectionPool` example)
  * ConnectionPool pool(...);
  *
  *	void thread()
  *	{
  *	  	auto connection = pool.get();
  *		connection->sendQuery("SELECT 'Hello, world!' AS world");
  *	}
  */
class IConnectionPool : private boost::noncopyable
{
public:
	using Entry = PoolBase<Connection>::Entry;

public:
	virtual ~IConnectionPool() {}

	/** Selects the connection to work. */
	Entry get(const Settings * settings = nullptr)
	{
		return doGet(settings);
	}

	/** Allocates up to the specified number of connections to work.
	  * Connections provide access to different replicas of one shard.
	  * If `get_all` flag is set, all connections are taken.
	  * Throws an exception if no connections can be selected.
	  */
	std::vector<Entry> getMany(const Settings * settings = nullptr,
		PoolMode pool_mode = PoolMode::GET_MANY)
	{
		return doGetMany(settings, pool_mode);
	}

protected:
	virtual Entry doGet(const Settings * settings) = 0;

	virtual std::vector<Entry> doGetMany(const Settings * settings, PoolMode pool_mode)
	{
		return std::vector<Entry>{ get(settings) };
	}
};

using ConnectionPoolPtr = std::shared_ptr<IConnectionPool>;
using ConnectionPools = std::vector<ConnectionPoolPtr>;
using ConnectionPoolsPtr = std::shared_ptr<ConnectionPools>;


/** A common connection pool, without fault tolerance.
  */
class ConnectionPool : public PoolBase<Connection>, public IConnectionPool
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
	Entry doGet(const Settings * settings) override
	{
		if (settings)
			return Base::get(settings->queue_max_wait_ms.totalMilliseconds());
		else
			return Base::get(-1);
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
	Protocol::Compression::Enum compression;		/// Whether to compress data when interacting with the server.

	Poco::Timespan connect_timeout;
	Poco::Timespan receive_timeout;
	Poco::Timespan send_timeout;
};

}
