#include <mysqlxx/Connection.h>
#include <mysqlxx/Exception.h>


namespace mysqlxx
{


Connection::Connection()
{
	is_connected = false;

	/// Инициализация библиотеки.
	LibrarySingleton::instance();
}

Connection::Connection(
	const char* db,
	const char* server,
	const char* user,
	const char* password,
	unsigned port,
	unsigned timeout,
	unsigned rw_timeout)
{
	is_connected = false;
	connect(db, server, user, password, port, timeout, rw_timeout);
}

Connection::~Connection()
{
	disconnect();
	mysql_thread_end();
}

void Connection::connect(const char* db,
	const char* server,
	const char* user,
	const char* password,
	unsigned port,
	unsigned timeout,
	unsigned rw_timeout)
{
	if (is_connected)
		disconnect();

	/// Инициализация библиотеки.
	LibrarySingleton::instance();

	if (!mysql_init(&driver))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	/// Установим таймауты
	if (mysql_options(&driver, MYSQL_OPT_CONNECT_TIMEOUT, reinterpret_cast<const char *>(&timeout)))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	if (mysql_options(&driver, MYSQL_OPT_READ_TIMEOUT, reinterpret_cast<const char *>(&rw_timeout)))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	if (mysql_options(&driver, MYSQL_OPT_WRITE_TIMEOUT, reinterpret_cast<const char *>(&rw_timeout)))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	/** Включаем возможность использовать запрос LOAD DATA LOCAL INFILE с серверами,
	  *  которые были скомпилированы без опции --enable-local-infile.
	  */
	if (mysql_options(&driver, MYSQL_OPT_LOCAL_INFILE, nullptr))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	if (!mysql_real_connect(&driver, server, user, password, db, port, nullptr, driver.client_flag))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	/// Установим кодировки по умолчанию - UTF-8.
	if (mysql_set_character_set(&driver, "UTF8"))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	/// Установим автоматический реконнект
	my_bool reconnect = true;
	if (mysql_options(&driver, MYSQL_OPT_RECONNECT, reinterpret_cast<const char *>(&reconnect)))
		throw ConnectionFailed(errorMessage(&driver), mysql_errno(&driver));

	is_connected = true;
}

bool Connection::connected() const
{
	return is_connected;
}

void Connection::disconnect()
{
	if (!is_connected)
		return;

	mysql_close(&driver);
	memset(&driver, 0, sizeof(driver));
	is_connected = false;
}

bool Connection::ping()
{
	return is_connected && !mysql_ping(&driver);
}

Query Connection::query(const std::string & str)
{
	return Query(this, str);
}

MYSQL * Connection::getDriver()
{
	return &driver;
}

}

