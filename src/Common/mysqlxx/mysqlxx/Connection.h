#pragma once

#include <memory>
#include <boost/noncopyable.hpp>

#include <Poco/Util/Application.h>


#include <mysqlxx/Query.h>
#include <mysqlxx/Exception.h>

#define MYSQLXX_DEFAULT_TIMEOUT 60
#define MYSQLXX_DEFAULT_RW_TIMEOUT 1800

/// Disable LOAD DATA LOCAL INFILE because it is insecure
#define MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE false
/// See https://dev.mysql.com/doc/c-api/5.7/en/c-api-auto-reconnect.html
#define MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT true


namespace mysqlxx
{


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
    Connection(
        const char * db,
        const char * server,
        const char * user = nullptr,
        const char * password = nullptr,
        unsigned port = 0,
        const char * socket = "",
        const char * ssl_ca = "",
        const char * ssl_cert = "",
        const char * ssl_key = "",
        unsigned timeout = MYSQLXX_DEFAULT_TIMEOUT,
        unsigned rw_timeout = MYSQLXX_DEFAULT_RW_TIMEOUT,
        bool enable_local_infile = MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE,
        bool opt_reconnect = MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT);

    /// Creates connection. Can be used if Poco::Util::Application is using.
    /// All settings will be got from config_name section of configuration.
    explicit Connection(const std::string & config_name);

    ~Connection();

    /// Provides delayed initialization or reconnection with other settings.
    void connect(const char * db,
        const char * server,
        const char * user,
        const char * password,
        unsigned port,
        const char * socket,
        const char* ssl_ca,
        const char* ssl_cert,
        const char* ssl_key,
        unsigned timeout = MYSQLXX_DEFAULT_TIMEOUT,
        unsigned rw_timeout = MYSQLXX_DEFAULT_RW_TIMEOUT,
        bool enable_local_infile = MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE,
        bool opt_reconnect = MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT);

    void connect(const std::string & config_name)
    {
        Poco::Util::LayeredConfiguration & cfg = Poco::Util::Application::instance().config();

        std::string db = cfg.getString(config_name + ".db", "");
        std::string server = cfg.getString(config_name + ".host");
        std::string user = cfg.getString(config_name + ".user");
        std::string password = cfg.getString(config_name + ".password");
        unsigned port = cfg.getInt(config_name + ".port", 0);
        std::string socket = cfg.getString(config_name + ".socket", "");
        std::string ssl_ca = cfg.getString(config_name + ".ssl_ca", "");
        std::string ssl_cert = cfg.getString(config_name + ".ssl_cert", "");
        std::string ssl_key = cfg.getString(config_name + ".ssl_key", "");
        bool enable_local_infile = cfg.getBool(config_name + ".enable_local_infile", MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE);
        bool opt_reconnect = cfg.getBool(config_name + ".opt_reconnect", MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT);

        unsigned timeout =
            cfg.getInt(config_name + ".connect_timeout",
                cfg.getInt("mysql_connect_timeout",
                    MYSQLXX_DEFAULT_TIMEOUT));

        unsigned rw_timeout =
            cfg.getInt(config_name + ".rw_timeout",
                cfg.getInt("mysql_rw_timeout",
                    MYSQLXX_DEFAULT_RW_TIMEOUT));

        connect(
                db.c_str(),
                server.c_str(),
                user.c_str(),
                password.c_str(),
                port,
                socket.c_str(),
                ssl_ca.c_str(),
                ssl_cert.c_str(),
                ssl_key.c_str(),
                timeout,
                rw_timeout,
                enable_local_infile,
                opt_reconnect);
    }

    /// If MySQL connection was established.
    bool connected() const;

    /// Disconnect from MySQL.
    void disconnect();

    /// Tries to reconnect if connection was lost. Is true if connection is established after call.
    bool ping();

    /// Creates query. It can be set with query string or later.
    Query query(const std::string & str);

    /// Get MySQL C API MYSQL object.
    MYSQL * getDriver();

private:
    std::unique_ptr<MYSQL> driver;
    bool is_initialized = false;
    bool is_connected = false;
};


}
