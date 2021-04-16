#if __has_include(<mysql.h>)
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif

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

auto & LibrarySingleton::instance()
{
    static LibrarySingleton instance;
    return instance;
}

Connection::Connection()
    : driver(std::make_unique<MYSQL>())
{
    /// MySQL library initialization.
    LibrarySingleton::instance();
}

Connection::Connection(const ConnectionConfiguration & configuration)
    : Connection()
{
    connect(configuration);
}

Connection::Connection(Connection && rhs) noexcept
    : driver(std::move(rhs.driver))
    , is_initialized(rhs.is_initialized)
    , is_connected(rhs.is_connected)
{
    rhs.driver = nullptr;
    rhs.is_connected = false;
    rhs.is_initialized = false;
}

Connection & Connection::operator=(Connection && rhs) noexcept
{
    disconnect();

    driver = std::move(rhs.driver);
    is_connected = rhs.is_connected;
    is_initialized = rhs.is_initialized;

    rhs.is_connected = false;
    rhs.is_initialized = false;
    rhs.driver = nullptr;

    return *this;
}

Connection::~Connection()
{
    disconnect();
}

bool Connection::tryConnect(const ConnectionConfiguration & configuration)
{
    if (is_connected)
        disconnect();

    if (!mysql_init(driver.get()))
        return false;

    is_initialized = true;

    /// Set timeouts.
    if (mysql_options(driver.get(), MYSQL_OPT_CONNECT_TIMEOUT, &configuration.timeout))
        return false;

    if (mysql_options(driver.get(), MYSQL_OPT_READ_TIMEOUT, &configuration.rw_timeout))
        return false;

    if (mysql_options(driver.get(), MYSQL_OPT_WRITE_TIMEOUT, &configuration.rw_timeout))
        return false;

    /// Disable LOAD DATA LOCAL INFILE because it is insecure if necessary.
    unsigned enable_local_infile_arg = static_cast<unsigned>(configuration.enable_local_infile);
    if (mysql_options(driver.get(), MYSQL_OPT_LOCAL_INFILE, &enable_local_infile_arg))
        return false;

    /// See C API Developer Guide: Automatic Reconnection Control
    if (mysql_options(driver.get(), MYSQL_OPT_RECONNECT, reinterpret_cast<const char *>(&configuration.opt_reconnect)))
        return false;

    /// Specifies particular ssl key and certificate if it needs
    if (mysql_ssl_set(driver.get(), ifNotEmpty(configuration.ssl_key.c_str()), ifNotEmpty(configuration.ssl_cert.c_str()), ifNotEmpty(configuration.ssl_ca.c_str()), nullptr, nullptr))
        return false;

    const char * server = configuration.server.c_str();
    const char * user = ifNotEmpty(configuration.user.c_str());
    const char * password = ifNotEmpty(configuration.password.c_str());
    const char * db = configuration.db.c_str();
    const char * socket = ifNotEmpty(configuration.socket.c_str());

    if (!mysql_real_connect(driver.get(), server, user, password, db, configuration.port, socket, driver->client_flag))
        return false;

    /// Sets UTF-8 as default encoding. See https://mariadb.com/kb/en/mysql_set_character_set/
    if (mysql_set_character_set(driver.get(), "utf8mb4"))
        return false;

    is_connected = true;

    return true;
}

void Connection::connect(const ConnectionConfiguration & configuration)
{
    bool result = tryConnect(configuration);

    if (!result)
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));
}

bool Connection::connected() const
{
    return is_connected;
}

void Connection::disconnect()
{
    if (!is_initialized)
        return;

    mysql_close(driver.get());
    memset(driver.get(), 0, sizeof(*driver));

    is_initialized = false;
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

