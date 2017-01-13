#include <mysql.h>
#include <mysqld_error.h>
#include <mysqlxx/Pool.h>


namespace mysqlxx
{

void Pool::Entry::incrementRefCount()
{
	if (!data)
		return;
	++data->ref_count;
	mysql_thread_init();
}

void Pool::Entry::decrementRefCount()
{
	if (!data)
		return;
	--data->ref_count;
	mysql_thread_end();
}


Pool::~Pool()
{
	Poco::ScopedLock<Poco::FastMutex> locker(lock);

	for (Connections::iterator it = connections.begin(); it != connections.end(); it++)
		delete static_cast<Connection *>(*it);
}


Pool::Entry Pool::Get()
{
	Poco::ScopedLock<Poco::FastMutex> locker(lock);

	initialize();
	for (;;)
	{
		for (Connections::iterator it = connections.begin(); it != connections.end(); it++)
		{
			if ((*it)->ref_count == 0)
				return Entry(*it, this);
		}

		if (connections.size() < static_cast<size_t>(max_connections))
		{
			Connection * conn = allocConnection();
			if (conn)
				return Entry(conn, this);
		}

		lock.unlock();
		::sleep(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);
		lock.lock();
	}
}


Pool::Entry Pool::tryGet()
{
	Poco::ScopedLock<Poco::FastMutex> locker(lock);

	initialize();

	/// Поиск уже установленного, но не использующегося сейчас соединения.
	for (Connections::iterator it = connections.begin(); it != connections.end(); ++it)
	{
		if ((*it)->ref_count == 0)
		{
			Entry res(*it, this);
			return res.tryForceConnected() ? res : Entry();
		}
	}

	/// Если пул переполнен.
	if (connections.size() >= max_connections)
		throw Poco::Exception("mysqlxx::Pool is full");

	/// Выделение нового соединения.
	Connection * conn = allocConnection(true);
	if (conn)
		return Entry(conn, this);

	return Entry();
}


void Pool::initialize()
{
	if (!initialized)
	{
		description = db + "@" + server + ":" + Poco::NumberFormatter::format(port) + " as user " + user;

		for (unsigned i = 0; i < default_connections; i++)
			allocConnection();

		initialized = true;
	}
}


Connection * Pool::allocConnection(bool dont_throw_if_failed_first_time)
{
	Poco::Util::Application & app = Poco::Util::Application::instance();

	std::unique_ptr<Connection> conn(new Connection);

	try
	{
		app.logger().information("MYSQL: Connecting to " + description);

		conn->conn.connect(
			db.c_str(),
						   server.c_str(),
						   user.c_str(),
						   password.c_str(),
						   port,
					 connect_timeout,
					 rw_timeout);
	}
	catch (mysqlxx::ConnectionFailed & e)
	{
		if ((!was_successful && !dont_throw_if_failed_first_time)
			|| e.errnum() == ER_ACCESS_DENIED_ERROR
			|| e.errnum() == ER_DBACCESS_DENIED_ERROR
			|| e.errnum() == ER_BAD_DB_ERROR)
		{
			app.logger().error(e.what());
			throw;
		}
		else
		{
			app.logger().error(e.what());
			return nullptr;
		}
	}

	was_successful = true;
	auto * connection = conn.release();
	connections.push_back(connection);
	return connection;
}

}
