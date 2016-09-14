#pragma once

#include <list>
#include <memory>

#include <mysql/mysqld_error.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/NumberFormatter.h>
#include <Poco/Mutex.h>
#include <Poco/Exception.h>

#include <common/logger_useful.h>
#include <mysqlxx/Connection.h>


#define MYSQLXX_POOL_DEFAULT_START_CONNECTIONS 	1
#define MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS 	16
#define MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL 		10


namespace mysqlxx
{

/** Пул соединений с MySQL.
  * Этот класс имеет мало отношения к mysqlxx и сделан не в стиле библиотеки. (взят из старого кода)
  * Использование:
  * 	mysqlxx::Pool pool("mysql_params");
  *
  *		void thread()
  *		{
  *		  	mysqlxx::Pool::Entry connection = pool.Get();
  *			std::string s = connection->query("SELECT 'Hello, world!' AS world").use().fetch()["world"].getString();
  *		}
  *
  * TODO: Упростить, используя PoolBase.
  */
class Pool final
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
		Entry() {}

		Entry(const Entry & src)
			: data(src.data), pool(src.pool)
		{
			incrementRefCount();
		}

		~Entry()
		{
			decrementRefCount();
		}

		Entry & operator= (const Entry & src)
		{
			pool = src.pool;
			if (data)
				decrementRefCount();
			data = src.data;
			if (data)
				incrementRefCount();
			return * this;
		}

		bool isNull() const
		{
			return data == nullptr;
		}

		operator mysqlxx::Connection & ()
		{
			if (data == nullptr)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return data->conn;
		}

		operator const mysqlxx::Connection & () const
		{
			if (data == nullptr)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return data->conn;
		}

		const mysqlxx::Connection * operator->() const
		{
			if (data == nullptr)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return &data->conn;
		}

		mysqlxx::Connection * operator->()
		{
			if (data == nullptr)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			forceConnected();
			return &data->conn;
		}

		Entry(Pool::Connection * conn, Pool * p)
			: data(conn), pool(p)
		{
			incrementRefCount();
		}

		std::string getDescription() const
		{
			if (pool)
				return pool->getDescription();
			else
				return "pool is null";
		}
		friend class Pool;

	private:
		/** Указатель на соединение. */
		Connection * data = nullptr;
		/** Указатель на пул, которому мы принадлежим. */
		Pool * pool = nullptr;

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
					::sleep(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);

				app.logger().information("MYSQL: Reconnecting to " + pool->description);
				data->conn.connect(
					pool->db.c_str(),
					pool->server.c_str(),
					pool->user.c_str(),
					pool->password.c_str(),
					pool->port,
					pool->connect_timeout,
					pool->rw_timeout);
			}
			while (!data->conn.ping());
		}

		/** Переподключается к базе данных в случае необходимости. Если не удалось - вернуть false. */
		bool tryForceConnected() const
		{
			return data->conn.ping();
		}


		void incrementRefCount()
		{
			if (!data)
				return;
			++data->ref_count;
			my_thread_init();
		}

		void decrementRefCount()
		{
			if (!data)
				return;
			--data->ref_count;
			my_thread_end();
		}
	};


	Pool(const std::string & config_name,
		unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
		const char * parent_config_name_ = nullptr)
		: Pool{
			Poco::Util::Application::instance().config(), config_name,
			default_connections_, max_connections_, parent_config_name_
		  }
	{}

	/**
	 * @param config_name			Имя параметра в конфигурационном файле
	 * @param default_connections_	Количество подключений по-умолчанию
	 * @param max_connections_		Максимальное количество подключений
	 */
	Pool(const Poco::Util::AbstractConfiguration & cfg, const std::string & config_name,
		 unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		 unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
		 const char * parent_config_name_ = nullptr)
		: default_connections(default_connections_), max_connections(max_connections_)
	{
		server 		= cfg.getString(config_name + ".host");

		if (parent_config_name_)
		{
			const std::string parent_config_name(parent_config_name_);
			db 		= cfg.getString(config_name + ".db", cfg.getString(parent_config_name + ".db", ""));
			user 	= cfg.has(config_name + ".user") ?
					cfg.getString(config_name + ".user") : cfg.getString(parent_config_name + ".user");
			password = cfg.has(config_name + ".password") ?
					cfg.getString(config_name + ".password") : cfg.getString(parent_config_name + ".password");
			port = cfg.has(config_name + ".port") ? cfg.getInt(config_name + ".port") :
					cfg.getInt(parent_config_name + ".port");
		}
		else
		{
			db 		= cfg.getString(config_name + ".db", "");
			user 	= cfg.getString(config_name + ".user");
			password =  cfg.getString(config_name + ".password");
			port = cfg.getInt(config_name + ".port");
		}

		connect_timeout = cfg.getInt(config_name + ".connect_timeout",
				cfg.getInt("mysql_connect_timeout",
					MYSQLXX_DEFAULT_TIMEOUT));

		rw_timeout =
			cfg.getInt(config_name + ".rw_timeout",
				cfg.getInt("mysql_rw_timeout",
					MYSQLXX_DEFAULT_RW_TIMEOUT));
	}

	/**
	 * @param db_					Имя БД
	 * @param server_				Хост для подключения
	 * @param user_					Имя пользователя
	 * @param password_				Пароль
	 * @param port_					Порт для подключения
	 * @param default_connections_	Количество подключений по-умолчанию
	 * @param max_connections_		Максимальное количество подключений
	 */
	Pool(const std::string & db_,
		 const std::string & server_,
		 const std::string & user_ = "",
		 const std::string & password_ = "",
		 unsigned port_ = 0,
		 unsigned connect_timeout_ = MYSQLXX_DEFAULT_TIMEOUT,
		 unsigned rw_timeout_ = MYSQLXX_DEFAULT_RW_TIMEOUT,
		 unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		 unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS)
	: default_connections(default_connections_), max_connections(max_connections_),
	db(db_), server(server_), user(user_), password(password_), port(port_),
	connect_timeout(connect_timeout_), rw_timeout(rw_timeout_) {}

	Pool(const Pool & other)
		: default_connections{other.default_connections},
		  max_connections{other.max_connections},
		  db{other.db}, server{other.server},
		  user{other.user}, password{other.password},
		  port{other.port}, connect_timeout{other.connect_timeout},
		  rw_timeout{other.rw_timeout}
	{}

	Pool & operator=(const Pool &) = delete;

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

private:
	/** Признак того, что мы инициализированы. */
	bool initialized{false};
	/** Список соединений. */
	using Connections = std::list<Connection *>;
	/** Список соединений. */
	Connections connections;
	/** Замок для доступа к списку соединений. */
	Poco::FastMutex lock;
	/** Описание соединения. */
	std::string description;

	/** Параметры подключения. **/
	std::string db;
	std::string server;
	std::string user;
	std::string password;
	unsigned port;
	unsigned connect_timeout;
	unsigned rw_timeout;

	/** Хотя бы один раз было успешное соединение. */
	bool was_successful{false};

	/** Выполняет инициализацию класса, если мы еще не инициализированы. */
	void initialize()
	{
		if (!initialized)
		{
			description = db + "@" + server + ":" + Poco::NumberFormatter::format(port) + " as user " + user;

			for (unsigned i = 0; i < default_connections; i++)
				allocConnection();

			initialized = true;
		}
	}

	/** Создает новое соединение. */
	Connection * allocConnection(bool dont_throw_if_failed_first_time = false)
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
};

}
