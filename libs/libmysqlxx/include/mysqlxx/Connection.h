#pragma once

#include <boost/noncopyable.hpp>

#include <Poco/Util/Application.h>

#include <Yandex/singleton.h>

#include <mysqlxx/Query.h>

#define MYSQLXX_DEFAULT_TIMEOUT 60
#define MYSQLXX_DEFAULT_RW_TIMEOUT 1800


namespace mysqlxx
{


/** Для корректной инициализации и деинициализации MySQL библиотеки.
  * Обеспечивает единственный и thread-safe вызов mysql_library_init().
  * Использование:
  * 	LibrarySingleton::instance();
  */
class LibrarySingleton : public Singleton<LibrarySingleton>
{
friend class Singleton<LibrarySingleton>;
private:
	LibrarySingleton()
	{
		if (mysql_library_init(0, nullptr, nullptr))
			throw Exception("Cannot initialize MySQL library.");
	}

	~LibrarySingleton()
	{
		mysql_library_end();
	}
};


/** Соединение с MySQL.
  * Использование:
  * 	mysqlxx::Connection connection("Test", "127.0.0.1", "root", "qwerty", 3306);
  *		std::cout << connection.query("SELECT 'Hello, World!'").store().at(0).at(0).getString() << std::endl;
  *
  * Или так, если вы используете конфигурацию из библиотеки Poco:
  *		mysqlxx::Connection connection("mysql_params");
  *
  * Внимание! Рекомендуется использовать соединение в том потоке, в котором оно создано.
  * Если вы используете соединение, созданное в другом потоке, то вы должны перед использованием
  *  вызвать функцию MySQL C API my_thread_init(), а после использования - my_thread_end().
  */
class Connection : private boost::noncopyable
{
public:
	/// Для отложенной инициализации.
	Connection();

	/// Создать соединение.
	Connection(
		const char* db,
		const char* server,
		const char* user = 0,
		const char* password = 0,
		unsigned port = 0,
		unsigned timeout = MYSQLXX_DEFAULT_TIMEOUT,
		unsigned rw_timeout = MYSQLXX_DEFAULT_RW_TIMEOUT);

	/** Конструктор-помошник. Создать соединение, считав все параметры из секции config_name конфигурации.
	  * Можно использовать, если вы используете Poco::Util::Application из библиотеки Poco.
	  */
	Connection(const std::string & config_name)
	{
		is_connected = false;
		connect(config_name);
	}

	virtual ~Connection();

	/// Для отложенной инициализации или для того, чтобы подключиться с другими параметрами.
	virtual void connect(const char * db,
		const char * server,
		const char * user,
		const char * password,
		unsigned port,
		unsigned timeout = MYSQLXX_DEFAULT_TIMEOUT,
		unsigned rw_timeout = MYSQLXX_DEFAULT_RW_TIMEOUT);

	void connect(const std::string & config_name)
	{
		Poco::Util::LayeredConfiguration & cfg = Poco::Util::Application::instance().config();

		std::string db 			= cfg.getString(config_name + ".db", "");
		std::string server 		= cfg.getString(config_name + ".host");
		std::string user 		= cfg.getString(config_name + ".user");
		std::string password	= cfg.getString(config_name + ".password");
		unsigned port			= cfg.getInt(config_name + ".port");

		unsigned timeout =
			cfg.getInt(config_name + ".connect_timeout",
				cfg.getInt("mysql_connect_timeout",
					MYSQLXX_DEFAULT_TIMEOUT));

		unsigned rw_timeout =
			cfg.getInt(config_name + ".rw_timeout",
				cfg.getInt("mysql_rw_timeout",
					MYSQLXX_DEFAULT_RW_TIMEOUT));

		connect(db.c_str(), server.c_str(), user.c_str(), password.c_str(), port, timeout, rw_timeout);
	}

	/// Было ли произведено соединение с MySQL.
	bool connected() const;

	/// Отсоединиться от MySQL.
	void disconnect();

	/// Если соединение утеряно - попытаться восстановить его. true - если после вызова соединение есть.
	bool ping();

	/// Создать запрос. Вы можете сразу указать его, передав строку, или заполнить потом.
	Query query(const std::string & str = "");

	/// Получить объект MYSQL из C API.
	MYSQL * getDriver();

private:
	MYSQL driver;
	bool is_connected;
};


}
