
#if USE_MYSQL
#include <mysql/mysql.h>
#endif

#include <mysqlxx/Connection.h>
#include <mysqlxx/Exception.h>


namespace mysqlxx
{

LibrarySingleton::LibrarySingleton()
{
#if USE_MYSQL
    if (mysql_library_init(0, nullptr, nullptr))
        throw Exception("Cannot initialize MySQL library.");
#endif
}

LibrarySingleton::~LibrarySingleton()
{
#if USE_MYSQL
    mysql_library_end();
#endif
}


Connection::Connection()
#if USE_MYSQL
    : driver(std::make_unique<MYSQL>())
#endif
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
#if USE_MYSQL
    : driver(std::make_unique<MYSQL>())
#endif
{
    is_connected = false;
    connect(db, server, user, password, port, timeout, rw_timeout);
}

Connection::Connection(const std::string & config_name)
#if USE_MYSQL
    : driver(std::make_unique<MYSQL>())
#endif
{
    is_connected = false;
    connect(config_name);
}

Connection::~Connection()
{
    disconnect();
#if USE_MYSQL
    mysql_thread_end();
#endif
}

void Connection::connect(const char* db,
    const char* server,
    const char* user,
    const char* password,
    unsigned port,
    unsigned timeout,
    unsigned rw_timeout)
{
#if USE_MYSQL
    if (is_connected)
        disconnect();

    /// Инициализация библиотеки.
    LibrarySingleton::instance();

    if (!mysql_init(driver.get()))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Установим таймауты
    if (mysql_options(driver.get(), MYSQL_OPT_CONNECT_TIMEOUT, reinterpret_cast<const char *>(&timeout)))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (mysql_options(driver.get(), MYSQL_OPT_READ_TIMEOUT, reinterpret_cast<const char *>(&rw_timeout)))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (mysql_options(driver.get(), MYSQL_OPT_WRITE_TIMEOUT, reinterpret_cast<const char *>(&rw_timeout)))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /** Включаем возможность использовать запрос LOAD DATA LOCAL INFILE с серверами,
      *  которые были скомпилированы без опции --enable-local-infile.
      */
    if (mysql_options(driver.get(), MYSQL_OPT_LOCAL_INFILE, nullptr))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (!mysql_real_connect(driver.get(), server, user, password, db, port, nullptr, driver->client_flag))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Установим кодировки по умолчанию - UTF-8.
    if (mysql_set_character_set(driver.get(), "UTF8"))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Установим автоматический реконнект
    my_bool reconnect = true;
    if (mysql_options(driver.get(), MYSQL_OPT_RECONNECT, reinterpret_cast<const char *>(&reconnect)))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    is_connected = true;
#endif
}

bool Connection::connected() const
{
    return is_connected;
}

void Connection::disconnect()
{
    if (!is_connected)
        return;

#if USE_MYSQL
    mysql_close(driver.get());
    memset(driver.get(), 0, sizeof(*driver));
#endif
    is_connected = false;
}

bool Connection::ping()
{
#if USE_MYSQL
    return is_connected && !mysql_ping(driver.get());
#else
    throw;
#endif
}

Query Connection::query(const std::string & str)
{
    return Query(this, str);
}

MYSQL * Connection::getDriver()
{
    return driver.get();
}

}

