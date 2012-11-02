#pragma once

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>
#include <Poco/Condition.h>

#include <DB/Client/Connection.h>


namespace DB
{

using Poco::SharedPtr;


namespace detail
{
	/** Соединение с флагом, используется ли оно сейчас. */
	struct PooledConnection
	{
		PooledConnection(Poco::Condition & available_,
			const String & host_, UInt16 port_, const String & default_database_,
			const DataTypeFactory & data_type_factory_,
			const String & client_name_ = "client",
			Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
			Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
			Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
			Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0))
			: conn(host_, port_, default_database_,
				data_type_factory_, client_name_, compression_,
				connect_timeout_, receive_timeout_, send_timeout_),
			in_use(false), available(available_)
		{
		}

		Connection conn;
		bool in_use;
		Poco::Condition & available;
	};


	/** Помошник, который устанавливает флаг использования соединения, а в деструкторе - снимает,
	  *  а также уведомляет о событии с помощью condvar-а.
	  */
	struct ConnectionPoolEntryHelper
	{
		ConnectionPoolEntryHelper(PooledConnection & data_) : data(data_) { data.in_use = true; }
		~ConnectionPoolEntryHelper() { data.in_use = false; data.available.signal(); }

		PooledConnection & data;
	};


	/** То, что выдаётся пользователю. */
	class ConnectionPoolEntry
	{
	public:
		ConnectionPoolEntry() {}	/// Для отложенной инициализации.
		ConnectionPoolEntry(PooledConnection & conn) : data(new ConnectionPoolEntryHelper(conn)) {}

		Connection * operator->() 				{ return &data->data.conn; }
		const Connection * operator->() const	{ return &data->data.conn; }
		Connection & operator*() 				{ return data->data.conn; }
		const Connection & operator*() const	{ return data->data.conn; }

		bool isNull() const { return data.isNull(); }

	private:
		Poco::SharedPtr<ConnectionPoolEntryHelper> data;
	};
}


/** Интерфейс для пулов соединений.
  *
  * Использование (на примере обычного ConnectionPool):
  * ConnectionPool pool(...);
  *
  *	void thread()
  *	{
  *	  	sqxxl::Pool::Entry connection = pool.get();
  *		connection->sendQuery("SELECT 'Hello, world!' AS world");
  *	}
  */
class IConnectionPool : private boost::noncopyable
{
public:
	typedef detail::ConnectionPoolEntry Entry;
	virtual Entry get() = 0;
};

typedef SharedPtr<IConnectionPool> ConnectionPoolPtr;
typedef std::vector<ConnectionPoolPtr> ConnectionPools;



/** Обычный пул соединений, без отказоустойчивости.
  * TODO: Неплохо бы обобщить все пулы, которые есть в проекте.
  */
class ConnectionPool : public IConnectionPool
{
public:
	ConnectionPool(unsigned max_connections_,
			const String & host_, UInt16 port_, const String & default_database_,
			const DataTypeFactory & data_type_factory_,
			const String & client_name_ = "client",
			Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
			Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
			Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
			Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0))
	   : max_connections(max_connections_), host(host_), port(port_), default_database(default_database_),
	    client_name(client_name_), compression(compression_), data_type_factory(data_type_factory_),
		connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_),
		log(&Logger::get("ConnectionPool (" + Poco::Net::SocketAddress(host, port).toString() + ")"))
	{
		connections.reserve(max_connections);
	}


	/** Выделяет соединение для работы. */
	Entry get()
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		while (true)
		{
			for (Connections::iterator it = connections.begin(); it != connections.end(); it++)
				if (!(*it)->in_use)
					return Entry(**it);

			if (connections.size() < max_connections)
				return Entry(allocConnection());

			LOG_INFO(log, "No free connections in pool. Waiting.");
			available.wait(mutex);
		}
	}

private:
	/** Максимально возможное количество соедиений. */
	unsigned max_connections;

	String host;
	UInt16 port;
	String default_database;

	String client_name;
	Protocol::Compression::Enum compression;		/// Сжимать ли данные при взаимодействии с сервером.

	const DataTypeFactory & data_type_factory;

	Poco::Timespan connect_timeout;
	Poco::Timespan receive_timeout;
	Poco::Timespan send_timeout;
	
	/** Список соединений. */
	typedef std::vector<Poco::SharedPtr<detail::PooledConnection> > Connections;
	Connections connections;

	/** Блокировка для доступа к списку соединений. */
	Poco::FastMutex mutex;
	Poco::Condition available;

	Logger * log;


	/** Создает новое соединение. */
	detail::PooledConnection & allocConnection()
	{
		connections.push_back(new detail::PooledConnection(
			available,
			host, port, default_database,
			data_type_factory, client_name, compression,
			connect_timeout, receive_timeout, send_timeout));
		
		return *connections.back();
	}
};

}
