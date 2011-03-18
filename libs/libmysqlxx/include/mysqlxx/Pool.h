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

/** @brief Пул соединений с MySQL.
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
	/** @brief Информация о соединении. */
	struct Connection
	{
		/** @brief Конструктор. */
		Connection()
			: RefCount(0)
		{
		}

		mysqlxx::Connection		Conn;
		int						RefCount;
	};

public:

	/** @brief Соединение с базой данных. */
	class Entry
	{
	public:
		/** @brief Конструктор по-умолчанию. */
		Entry()
			: Data(NULL), pool(NULL)
		{
		}

		/** @brief Конструктор копирования. */
		Entry(const Entry & src)
			: Data(src.Data), pool(src.pool)
		{
			if (Data)
				Data->RefCount++;
		}

		/** @brief Деструктор. */
		virtual ~Entry()
		{
			if (Data)
				Data->RefCount--;
		}

		/** @brief Оператор присваивания. */
		Entry & operator= (const Entry & src)
		{
			pool = src.pool;
			if (Data)
				Data->RefCount--;
			Data = src.Data;
			if (Data)
				Data->RefCount++;
			return * this;
		}

		/** @brief Оператор доступа к вложенному объекту. */
		operator mysqlxx::Connection & ()
		{
			if (Data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			ForceConnected();
			return Data->Conn;
		}

		/** @brief Оператор доступа к вложенному объекту. */
		operator const mysqlxx::Connection & () const
		{
			if (Data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			ForceConnected();
			return Data->Conn;
		}

		/** @brief Оператор доступа к вложенному объекту. */
		const mysqlxx::Connection * operator->() const
		{
			if (Data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			ForceConnected();
			return &Data->Conn;
		}

		/** @brief Оператор доступа к вложенному объекту. */
		mysqlxx::Connection * operator->()
		{
			if (Data == NULL)
				throw Poco::RuntimeException("Tried to access NULL database connection.");
			ForceConnected();
			return &Data->Conn;
		}

		/** @brief Конструктор */
		Entry(Pool::Connection * Conn, Pool * p)
			: Data(Conn), pool(p)
		{
			if (Data)
				Data->RefCount++;
		}

		friend class Pool;

	private:
		/** @brief Указатель на соединение. */
		Connection * Data;
		/** @brief Указатель на пул, которому мы принадлежим. */
		Pool * pool;

		/** @brief Переподключается к базе данных в случае необходимости. */
		void ForceConnected() const
		{
			Poco::Util::Application & app = Poco::Util::Application::instance();

			if (Data->Conn.ping())
				return;

			bool first = true;
			do
			{
				if (first)
					first = false;
				else
					::sleep(5);

				app.logger().information("MYSQL: Reconnecting to " + pool->DBName + "@" +
					pool->DBHost + ":" + Poco::NumberFormatter::format(pool->DBPort) + " as user " + pool->DBUser);
				Data->Conn.connect(pool->DBName.c_str(), pool->DBHost.c_str(), pool->DBUser.c_str(),
					pool->DBPass.c_str(), pool->DBPort);
			}
			while (!Data->Conn.ping());

			pool->afterConnect(Data->Conn);
		}
	};

	/**
	 * @brief Конструктор.
	 * @param ConfigName		Имя параметра в конфигурационном файле.
	 * @param DefConn			Количество подключений по-умолчанию
	 * @param MaxConn			Максимальное количество подключений
	 * @param AllowMultiQueries	Не используется.
	 */
	Pool(const std::string & ConfigName,
		 int DefConn = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
		 int MaxConn = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
			const std::string & InitConnect_ = "")
		: DefaultConnections(DefConn), MaxConnections(MaxConn), InitConnect(InitConnect_),
		Initialized(false), CfgName(ConfigName)
	{
	}

	/** @brief Деструктор. */
	~Pool()
	{
		Poco::ScopedLock<Poco::FastMutex> Locker(Lock);

		for (ConnList::iterator it = Connections.begin(); it != Connections.end(); it++)
			delete static_cast<Connection *>(*it);
	}

	/** @brief Выделяет соединение для работы. */
	Entry Get()
	{
		Poco::ScopedLock<Poco::FastMutex> Locker(Lock);

		Initialize();
		for (;;)
		{
			for (ConnList::iterator it = Connections.begin(); it != Connections.end(); it++)
			{
				if ((*it)->RefCount == 0)
					return Entry(*it, this);
			}

			if (Connections.size() < (size_t)MaxConnections)
			{
				Connection * Conn = AllocConnection();
				if (Conn)
				{
					return Entry(Conn, this);
				}
			}

			Lock.unlock();
			sched_yield();
			::sleep(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);
			Lock.lock();
		}
	}

	/** @brief Возвращает имя базы данных. */
	const std::string & DatabaseName()
	{
		Poco::ScopedLock<Poco::FastMutex> Locker(Lock);

		Initialize();
		return DBName;
	}

protected:
	/** @brief Количество соединений с MySQL, создаваемых при запуске. */
	int DefaultConnections;
	/** @brief Максимально возможное количество соедиений. */
	int MaxConnections;
	/** @brief Запрос, выполняющийся сразу после соединения с БД. Пример: "SET NAMES cp1251". */
	std::string InitConnect;

private:
	/** @brief Признак того, что мы инициализированы. */
	bool Initialized;
	/** @brief Список соединений. */
	typedef std::list<Connection *> ConnList;
	/** @brief Список соединений. */
	ConnList Connections;
	/** @brief Замок для доступа к списку соединений. */
	Poco::FastMutex Lock;
	/** @brief Имя раздела в конфигурационном файле. */
	std::string CfgName;
	/** @brief Имя сервера базы данных. */
	std::string DBHost;
	/** @brief Порт сервера базы данных. */
	int DBPort;
	/** @brief Имя пользователя базы данных. */
	std::string DBUser;
	/** @brief Пароль пользователя базы данных. */
	std::string DBPass;
	/** @brief Имя базы данных. */
	std::string DBName;

	/** @brief Выполняет инициализацию класса, если мы еще не инициализированы. */
	inline void Initialize()
	{
		if (!Initialized)
		{
			Poco::Util::Application & app = Poco::Util::Application::instance();
			Poco::Util::LayeredConfiguration & cfg = app.config();

			DBHost = cfg.getString(CfgName + ".host");
			DBPort = cfg.getInt(CfgName + ".port");
			DBUser = cfg.getString(CfgName + ".user");
			DBPass = cfg.getString(CfgName + ".password");
			DBName = cfg.getString(CfgName + ".db", "");

			for (int i = 0; i < DefaultConnections; i++)
				AllocConnection();

			Initialized = true;
		}
	}

	/** @brief Создает новое соединение. */
	Connection * AllocConnection()
	{
		Poco::Util::Application & app = Poco::Util::Application::instance();
		Connection * Conn;

		Conn = new Connection();
		try
		{
			app.logger().information("MYSQL: Connecting to " + DBName + "@" +
				DBHost + ":" + Poco::NumberFormatter::format(DBPort) + " as user " + DBUser);
			Conn->Conn.connect(DBName.c_str(), DBHost.c_str(), DBUser.c_str(), DBPass.c_str(), DBPort);
		}
		catch (mysqlxx::ConnectionFailed & e)
		{
			if (e.errnum() == ER_ACCESS_DENIED_ERROR
				|| e.errnum() == ER_DBACCESS_DENIED_ERROR
				|| e.errnum() == ER_BAD_DB_ERROR)
			{
				app.logger().error(e.what());
				throw;
			}
			else
			{
				app.logger().error(e.what());
				delete Conn;

				if (Daemon::instance().isCancelled())
					throw Poco::Exception("Daemon is cancelled while trying to connect to MySQL server.");

				return NULL;
			}
		}

		afterConnect(Conn->Conn);
		Connections.push_back(Conn);
		return Conn;
	}


	/** @brief Действия, выполняемые после соединения. */
	void afterConnect(mysqlxx::Connection & Conn)
	{
		Poco::Util::Application & app = Poco::Util::Application::instance();

		/// Инициализирующий запрос (например, установка другой кодировки)
		if (!InitConnect.empty())
		{
			mysqlxx::Query Q = Conn.query();
			Q << InitConnect;
			app.logger().trace(Q.str());
			Q.execute();
		}
	}
};

}

#endif
