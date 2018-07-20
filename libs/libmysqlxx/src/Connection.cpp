#include <mysql/mysql.h>

#include <mysqlxx/Connection.h>
#include <mysqlxx/Exception.h>

static inline const char* ifNotEmpty(const char* s)
{
    return s && *s ? s : nullptr;
}

namespace mysqlxx
{

LibrarySingleton::LibrarySingleton()
{
    if (mysql_library_init(0, nullptr, nullptr))
        throw Exception("Cannot initialize MySQL library.");
}

LibrarySingleton::~LibrarySingleton()
{
    mysql_library_end();
}


Connection::Connection()
    : driver(std::make_unique<MYSQL>())
{
    is_connected = false;

    /// MySQL library initialization.
    LibrarySingleton::instance();
}

Connection::Connection(
    const char* db,
    const char* server,
    const char* user,
    const char* password,
    unsigned port,
    const char * socket,
    const char* ssl_ca,
    const char* ssl_cert,
    const char* ssl_key,
    unsigned timeout,
    unsigned rw_timeout)
    : driver(std::make_unique<MYSQL>())
{
    is_connected = false;
    connect(db, server, user, password, port, socket, ssl_ca, ssl_cert, ssl_key, timeout, rw_timeout);
}

Connection::Connection(const std::string & config_name)
    : driver(std::make_unique<MYSQL>())
{
    is_connected = false;
    connect(config_name);
}

Connection::~Connection()
{
    disconnect();
    mysql_thread_end();
}

void Connection::connect(const char* db,
    const char * server,
    const char * user,
    const char * password,
    unsigned port,
    const char * socket,
    const char * ssl_ca,
    const char * ssl_cert,
    const char * ssl_key,
    unsigned timeout,
    unsigned rw_timeout)
{
    if (is_connected)
        disconnect();

    /// MySQL library initialization.
    LibrarySingleton::instance();

    if (!mysql_init(driver.get()))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Set timeouts.
    if (mysql_options(driver.get(), MYSQL_OPT_CONNECT_TIMEOUT, &timeout))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (mysql_options(driver.get(), MYSQL_OPT_READ_TIMEOUT, &rw_timeout))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (mysql_options(driver.get(), MYSQL_OPT_WRITE_TIMEOUT, &rw_timeout))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Disable LOAD DATA LOCAL INFILE because it is insecure.
    unsigned enable_local_infile = 0;
    if (mysql_options(driver.get(), MYSQL_OPT_LOCAL_INFILE, &enable_local_infile))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Specifies particular ssl key and certificate if it needs
    if (mysql_ssl_set(driver.get(), ifNotEmpty(ssl_key), ifNotEmpty(ssl_cert), ifNotEmpty(ssl_ca), nullptr, nullptr))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (!mysql_real_connect(driver.get(), server, user, password, db, port, ifNotEmpty(socket), driver->client_flag))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Sets UTF-8 as default encoding.
    if (mysql_set_character_set(driver.get(), "UTF8"))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Enables auto-reconnect.
    my_bool reconnect = true;
    if (mysql_options(driver.get(), MYSQL_OPT_RECONNECT, reinterpret_cast<const char *>(&reconnect)))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

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

    mysql_close(driver.get());
    memset(driver.get(), 0, sizeof(*driver));
    is_connected = false;
}

bool Connection::ping()
{
    return is_connected && !mysql_ping(driver.get());
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

