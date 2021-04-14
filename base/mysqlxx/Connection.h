#pragma once

#include <memory>
#include <boost/noncopyable.hpp>

#include <Poco/Util/Application.h>


#include <mysqlxx/Query.h>
#include <mysqlxx/Exception.h>

namespace mysqlxx
{

static constexpr size_t DEFAULT_TIMEOUT = 60;
static constexpr size_t DEFAULT_RW_TIMEOUT = 1800;
/// Disable LOAD DATA LOCAL INFILE because it is insecure
static constexpr bool DEFAULT_ENABLE_LOCAL_INFILE = false;
/// See https://dev.mysql.com/doc/c-api/5.7/en/c-api-auto-reconnect.html
static constexpr bool DEFAULT_MYSQL_OPT_RECONNECT = true;

struct ConnectionConfiguration
{
    const std::string db;
    const std::string server;

    const unsigned port = 0;
    const std::string socket;

    const std::string user;
    const std::string password;

    const size_t connect_timeout = DEFAULT_TIMEOUT;

    const bool enable_local_infile = DEFAULT_ENABLE_LOCAL_INFILE;
    const bool opt_reconnect = DEFAULT_MYSQL_OPT_RECONNECT;

    const std::string ssl_ca;
    const std::string ssl_cert;
    const std::string ssl_key;

    unsigned timeout = DEFAULT_TIMEOUT;
    unsigned rw_timeout = DEFAULT_RW_TIMEOUT;

    std::string getDescription() const
    {
        return db + "@" + server + ":" + std::to_string(port) + " as user " + user;
    }

    std::string getIdentity() const
    {
        auto result = db + server + std::to_string(port) + socket + user + std::to_string(connect_timeout) + std::to_string(enable_local_infile);
        result += std::to_string(opt_reconnect) + ssl_ca + ssl_cert + ssl_key + std::to_string(timeout) + std::to_string(rw_timeout);

        return result;
    }
};

/** LibrarySingleton is used for appropriate initialisation and deinitialisation of MySQL library.
  * Makes single thread-safe call of mysql_library_init().
  * Usage:
  *     LibrarySingleton::instance();
  */
class LibrarySingleton : private boost::noncopyable
{
public:
    static auto & instance();

private:
    LibrarySingleton();
    ~LibrarySingleton();
};


/** MySQL connection.
  * Usage:
  *        mysqlxx::Connection connection("Test", "127.0.0.1", "root", "qwerty", 3306);
  *
  * Or with Poco library configuration:
  *        mysqlxx::Connection connection("mysql_params");
  *
  * Or using socket:
  *        mysqlxx::Connection connection("Test", "localhost", "root", "qwerty", 0, "/path/to/socket/file.sock");
  *
  * Or using custom certificate authority file:
  *        mysqlxx::Connection connection("Test", "localhost", "root", "qwerty", 3306, "/path/to/ca/file.pem");
  *
  * Or using custom certificate and key file:
  *        mysqlxx::Connection connection("Test", "localhost", "root", "qwerty", 3306, "", "/path/to/cert/file.pem", "/path/to/key/file.pem");
  *
  * Attention! It's strictly recommended to use connection in thread where it was created.
  * In order to use connection in other thread, you should call MySQL C API function mysql_thread_init() before and
  * mysql_thread_end() after working with it.
  */
class Connection final : private boost::noncopyable
{
public:
    /// For delayed initialisation
    Connection();

    /// Creates connection. Either port either socket should be specified.
    /// If server is localhost and socket is not empty, than socket is used. Otherwise, server and port is used.
    explicit Connection(ConnectionConfiguration & connection_configuration);

    Connection(Connection && rhs) noexcept;

    Connection & operator=(Connection && rhs) noexcept;

    ~Connection();

    bool tryConnect(ConnectionConfiguration & connection_configuration);

    /// Provides delayed initialization or reconnection with other settings.
    void connect(ConnectionConfiguration & connection_configuration);

    /// If MySQL connection was established.
    bool connected() const;

    /// Disconnect from MySQL.
    void disconnect();

    /// Tries to reconnect if connection was lost. Is true if connection is established after call.
    bool ping();

    /// Creates query. It can be set with query string or later.
    Query query(const std::string & str = "");

    /// Get MySQL C API MYSQL object.
    MYSQL * getDriver();

    bool isInitialized() const
    {
        return is_initialized;
    }

    bool isConnected() const
    {
        return is_connected;
    }

private:
    std::unique_ptr<MYSQL> driver;
    bool is_initialized = false;
    bool is_connected = false;
};


}
