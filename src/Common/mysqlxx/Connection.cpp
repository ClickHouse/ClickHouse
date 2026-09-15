#if __has_include(<mysql.h>)
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif

#include <mysqlxx/Connection.h>
#include <mysqlxx/Exception.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <algorithm>
#include <cerrno>
#include <ctime>
#include <poll.h>

static inline const char* ifNotEmpty(const char* s)
{
    return s && *s ? s : nullptr;
}

namespace
{

/// Replacement for the connector's own pvio_socket_wait_io_or_timeout, installed via
/// MARIADB_OPT_IO_WAIT. Contract: `timeout` is in milliseconds and <= 0 means wait indefinitely;
/// a return value below 1 is a failure, 1 means the socket is ready.
int cancellationAwareIoWait(my_socket handle, my_bool is_read, int timeout) noexcept
{
    static constexpr int slice_ms = 200;

    try
    {
        pollfd poll_fd{};
        poll_fd.fd = handle;
        poll_fd.events = is_read ? POLLIN : POLLOUT;

        const bool bounded = timeout > 0;
        timespec start{};
        if (bounded)
            clock_gettime(CLOCK_MONOTONIC, &start);

        int remaining = timeout;
        while (true)
        {
            const int rc = poll(&poll_fd, 1, bounded ? std::min(remaining, slice_ms) : slice_ms);

            /// Ready, or a failure other than a signal: hand it back untouched, errno included.
            if (rc != 0 && !(rc == -1 && errno == EINTR))
                return rc;

            if (DB::CurrentThread::isInitialized() && DB::CurrentThread::get().isQueryCanceled())
                return 0;

            /// Recompute the budget from the start of the wait, so neither a signal nor a
            /// slice boundary extends the deadline. An unbounded wait has no budget to spend.
            if (bounded)
            {
                timespec now{};
                clock_gettime(CLOCK_MONOTONIC, &now);
                const int64_t elapsed_ms
                    = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
                remaining = timeout - static_cast<int>(elapsed_ms);
                if (remaining <= 0)
                {
                    errno = ETIMEDOUT;
                    return 0;
                }
            }
        }
    }
    /// Ok: invoked from C, so nothing may escape through the connector's frames. The only throwing
    /// call is the cancellation predicate, and 0 is the failure value the caller already handles.
    catch (...)
    {
        return 0;
    }
}

/// Scoped to connection establishment; post-connect reads and writes keep the library's own wait.
struct ScopedCancellationAwareIoWait
{
    MYSQL * driver;

    explicit ScopedCancellationAwareIoWait(MYSQL * driver_) : driver(driver_)
    {
        mysql_options(driver, MARIADB_OPT_IO_WAIT, reinterpret_cast<const void *>(&cancellationAwareIoWait));
    }

    ~ScopedCancellationAwareIoWait() { mysql_options(driver, MARIADB_OPT_IO_WAIT, nullptr); }
};

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
    unsigned rw_timeout,
    bool enable_local_infile,
    bool opt_reconnect,
    bool enable_compression)
    : Connection()
{
    connect(db, server, user, password, port, socket, ssl_ca, ssl_cert, ssl_key, timeout, rw_timeout, enable_local_infile, opt_reconnect, enable_compression);
}

Connection::Connection(const std::string & config_name)
    : Connection()
{
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
    unsigned rw_timeout,
    bool enable_local_infile,
    bool opt_reconnect,
    bool enable_compression)
{
    if (is_connected)
        disconnect();

    if (!mysql_init(driver.get()))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));
    is_initialized = true;

    /// Set timeouts.
    if (mysql_options(driver.get(), MYSQL_OPT_CONNECT_TIMEOUT, &timeout))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (mysql_options(driver.get(), MYSQL_OPT_READ_TIMEOUT, &rw_timeout))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    if (mysql_options(driver.get(), MYSQL_OPT_WRITE_TIMEOUT, &rw_timeout))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Disable LOAD DATA LOCAL INFILE because it is insecure if necessary.
    unsigned enable_local_infile_arg = static_cast<unsigned>(enable_local_infile);
    if (mysql_options(driver.get(), MYSQL_OPT_LOCAL_INFILE, &enable_local_infile_arg))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// See C API Developer Guide: Automatic Reconnection Control
    if (mysql_options(driver.get(), MYSQL_OPT_RECONNECT, reinterpret_cast<const char *>(&opt_reconnect)))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Enable classic MySQL protocol compression if requested.
    if (enable_compression && mysql_options(driver.get(), MYSQL_OPT_COMPRESS, nullptr))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    /// Specifies particular ssl key and certificate if it needs
    if (mysql_ssl_set(driver.get(), ifNotEmpty(ssl_key), ifNotEmpty(ssl_cert), ifNotEmpty(ssl_ca), nullptr, nullptr))
        throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

    {
        ScopedCancellationAwareIoWait io_wait_guard(driver.get());

        if (!mysql_real_connect(driver.get(), server, user, password, db, port, ifNotEmpty(socket), driver->client_flag))
            throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));

        /// Sets UTF-8 as default encoding. See https://mariadb.com/kb/en/mysql_set_character_set/
        if (mysql_set_character_set(driver.get(), "utf8mb4"))
            throw ConnectionFailed(errorMessage(driver.get()), mysql_errno(driver.get()));
    }

    is_connected = true;
}

bool Connection::connected() const
{
    return is_connected;
}

void Connection::disconnect()
{
    if (!is_initialized)
        return;

    // If driver->free_me, then mysql_close will deallocate memory by calling 'free' function.
    chassert(driver && !driver->free_me);
    mysql_close(driver.get());
    memset(driver.get(), 0, sizeof(*driver));

    is_initialized = false;
    is_connected = false;
}

bool Connection::ping()
{
    if (!is_connected)
        return false;

    /// With MYSQL_OPT_RECONNECT set, a dropped link makes ma_simple_command reconnect via
    /// mariadb_reconnect -> mysql_real_connect, so a ping can block like a connect.
    ScopedCancellationAwareIoWait io_wait_guard(driver.get());
    return !mysql_ping(driver.get());
}

Query Connection::query(const std::string & str)
{
    return Query(this, str);
}

MYSQL * Connection::getDriver()
{
    return driver.get();
}

uint64_t Connection::getDriverThreadID()
{
    return mysql_thread_id(driver.get());
}

}
