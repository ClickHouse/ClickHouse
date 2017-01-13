#pragma once

#include <list>
#include <memory>

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
		mysqlxx::Connection conn;
		int ref_count = 0;
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
			forceConnected();
			return data->conn;
		}

		operator const mysqlxx::Connection & () const
		{
			forceConnected();
			return data->conn;
		}

		const mysqlxx::Connection * operator->() const
		{
			forceConnected();
			return &data->conn;
		}

		mysqlxx::Connection * operator->()
		{
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
			if (data == nullptr)
				throw Poco::RuntimeException("Tried to access NULL database connection.");

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


		void incrementRefCount();
		void decrementRefCount();
	};


	Pool(const std::string & config_name,
		unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
		const char * parent_config_name_ = nullptr)
		: Pool{Poco::Util::Application::instance().config(), config_name,
			default_connections_, max_connections_, parent_config_name_}
	{}

	/**
	 * @param config_name			Имя параметра в конфигурационном файле
	 * @param default_connections_	Количество подключений по-умолчанию
	 * @param max_connections_		Максимальное количество подключений
	 */
	Pool(const Poco::Util::AbstractConfiguration & cfg, const std::string & config_name,
		 unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		 unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
		 const char * parent_config_name_ = nullptr);

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

	~Pool();

	/** Выделяет соединение для работы. */
	Entry Get();

	/** Выделяет соединение для работы.
	  * Если база недоступна - возвращает пустой объект Entry.
	  * Если пул переполнен - кидает исключение.
	  */
	Entry tryGet();

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
	void initialize();

	/** Create new connection. */
	Connection * allocConnection(bool dont_throw_if_failed_first_time = false);
};

}
