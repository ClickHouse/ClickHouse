#ifndef MYSQLXX_POOL_H
#define MYSQLXX_POOL_H

#include <list>

#include <mysql/mysqld_error.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/NumberFormatter.h>
#include <Poco/Mutex.h>
#include <Poco/Exception.h>
#include <Poco/SharedPtr.h>

#include <Yandex/logger_useful.h>
#include <Yandex/daemon.h>

#include <Yandex/daemon.h>

#include <mysqlxx/Connection.h>


#define MYSQLXX_POOL_DEFAULT_START_CONNECTIONS 	1
#define MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS 	16
#define MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL 		10


namespace mysqlxx
{

/** Пул соединений с MySQL.
  * Этот класс имеет мало отношения в mysqlxx и сделан не в стиле библиотеки. (взят из старого кода)
  * Использование:
  * 	mysqlxx::Pool pool("mysql_params");
  *
  *		void thread()
  *		{
  *		  	mysqlxx::Pool::Entry connection = pool.Get();
  *			std::string s = connection->query("SELECT 'Hello, world!' AS world").use().fetch()["world"].getString();
  *		}
  */
class Pool
{
protected:
	/** Информация о соединении. */
	struct Connection
	{
		Connection() : ref_count(0) {}

		mysqlxx::Connection conn;
		int ref_count;
	};

public:
	/** Соединение с базой данных. */
	class Entry
	{
	public:
		Entry() : data(NULL), pool(NULL) {}

		Entry(const Entry & src)
			: data(src.data), pool(src.pool)
		{
			if (data)
				++data->ref_count;
		}

		~Entry()
		{
			if (data)
				--data->ref_count;
		}

		Entry & operator= (const Entry & src)
		{
			pool = src.pool;
			if (data)
				--data->ref_count;
			data = src.data;
			if (data)
				++data->ref_count;
			return * this;
		}

		bool isNull() const
		{
			return data == NULL;
		}

		operator mysqlxx::Connection & ()
		{
			if (data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return data->conn;
		}

		operator const mysqlxx::Connection & () const
		{
			if (data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return data->conn;
		}

		const mysqlxx::Connection * operator->() const
		{
			if (data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return &data->conn;
		}

		mysqlxx::Connection * operator->()
		{
			if (data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return &data->conn;
		}

		Entry(Pool::Connection * conn, Pool * p)
			: data(conn), pool(p)
		{
			if (data)
				data->ref_count++;
		}

		friend class Pool;

	private:
		/** Указатель на соединение. */
		Connection * data;
		/** Указатель на пул, которому мы принадлежим. */
		Pool * pool;

		/** Переподключается к базе данных в случае необходимости. Если не удалось - подождать и попробовать снова. */
		void forceConnected() const
		{
			Poco::Util::Application & app = Poco::Util::Application::instance();

			if (data->conn.ping())
				return;

			bool first = true;
			do
			{
				if (first)
					first = false;
				else
					Daemon::instance().sleep(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);

				app.logger().information("MYSQL: Reconnecting to " + pool->description);
				data->conn.connect(pool->config_name);
			}
			while (!data->conn.ping());

			pool->afterConnect(data->conn);
		}

		/** Переподключается к базе данных в случае необходимости. Если не удалось - вернуть false. */
		bool tryForceConnected() const
		{
			return data->conn.ping();
		}
	};

	/**
	 * @param ConfigName		Имя параметра в конфигурационном файле.
	 * @param DefConn			Количество подключений по-умолчанию
	 * @param MaxConn			Максимальное количество подключений
	 * @param AllowMultiQueries	Не используется.
	 */
	Pool(const std::string & config_name_,
		 unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		 unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
			const std::string & init_connect_ = "")
		: default_connections(default_connections_), max_connections(max_connections_), init_connect(init_connect_),
		initialized(false), config_name(config_name_), was_successful(false)
	{
	}

	~Pool()
	{
		Poco::ScopedLock<Poco::FastMutex> locker(lock);

		for (Connections::iterator it = connections.begin(); it != connections.end(); it++)
			delete static_cast<Connection *>(*it);
	}

	/** Выделяет соединение для работы. */
	Entry Get()
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

			if (connections.size() < (size_t)max_connections)
			{
				Connection * conn = allocConnection();
				if (conn)
					return Entry(conn, this);
			}

			lock.unlock();
			sched_yield();
			Daemon::instance().sleep(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);
			lock.lock();
		}
	}

	/** Выделяет соединение для работы.
	  * Если база недоступна - возвращает пустой объект Entry.
	  * Если пул переполнен - кидает исключение.
	  */
	Entry tryGet()
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


	/// Получить описание БД
	std::string getDescription() const
	{
		return description;
	}

protected:
	/** Количество соединений с MySQL, создаваемых при запуске. */
	unsigned default_connections;
	/** Максимально возможное количество соедиений. */
	unsigned max_connections;
	/** Запрос, выполняющийся сразу после соединения с БД. Пример: "SET NAMES cp1251". */
	std::string init_connect;

private:
	/** Признак того, что мы инициализированы. */
	bool initialized;
	/** Список соединений. */
	typedef std::list<Connection *> Connections;
	/** Список соединений. */
	Connections connections;
	/** Замок для доступа к списку соединений. */
	Poco::FastMutex lock;
	/** Имя раздела в конфигурационном файле. */
	std::string config_name;
	/** Описание соединения. */
	std::string description;

	/** Хотя бы один раз было успешное соединение. */
	bool was_successful;

	/** Выполняет инициализацию класса, если мы еще не инициализированы. */
	inline void initialize()
	{
		if (!initialized)
		{
			Poco::Util::Application & app = Poco::Util::Application::instance();
			Poco::Util::LayeredConfiguration & cfg = app.config();

			description = cfg.getString(config_name + ".db", "")
				+ "@" + cfg.getString(config_name + ".host")
				+ ":" + cfg.getString(config_name + ".port")
				+ " as user " + cfg.getString(config_name + ".user");

			for (unsigned i = 0; i < default_connections; i++)
				allocConnection();

			initialized = true;
		}
	}

	/** Создает новое соединение. */
	Connection * allocConnection(bool dont_throw_if_failed_first_time = false)
	{
		Poco::Util::Application & app = Poco::Util::Application::instance();
		Connection * conn;

		conn = new Connection();
		try
		{
			app.logger().information("MYSQL: Connecting to " + description);
			conn->conn.connect(config_name);
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
				delete conn;

				if (Daemon::instance().isCancelled())
					throw Poco::Exception("Daemon is cancelled while trying to connect to MySQL server.");

				return NULL;
			}
		}

		was_successful = true;
		afterConnect(conn->conn);
		connections.push_back(conn);
		return conn;
	}


	/** Действия, выполняемые после соединения. */
	void afterConnect(mysqlxx::Connection & conn)
	{
		Poco::Util::Application & app = Poco::Util::Application::instance();

		/// Инициализирующий запрос (например, установка другой кодировки)
		if (!init_connect.empty())
		{
			mysqlxx::Query q = conn.query();
			q << init_connect;
			app.logger().trace(q.str());
			q.execute();
		}
	}
};

}

#endif
