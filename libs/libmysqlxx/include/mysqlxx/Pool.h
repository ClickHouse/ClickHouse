#pragma once

#include <list>
#include <memory>
#include <mutex>

#include <Poco/Exception.h>
#include <mysqlxx/Connection.h>


#define MYSQLXX_POOL_DEFAULT_START_CONNECTIONS 1
#define MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS 16
#define MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL 1


namespace mysqlxx
{

/** MySQL connections pool.
 * This class is poorly connected with mysqlxx and is made in different style (was taken from old code).
 * Usage:
 *        mysqlxx::Pool pool("mysql_params");
 *
 *        void thread()
 *        {
 *              mysqlxx::Pool::Entry connection = pool.Get();
 *            std::string s = connection->query("SELECT 'Hello, world!' AS world").use().fetch()["world"].getString();
 *        }
 * TODO: simplify with PoolBase.
 */
class Pool final
{
protected:
    /// Information about connection.
    struct Connection
    {
        mysqlxx::Connection conn;
        int ref_count = 0;
    };

public:
    /// Connection with database.
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

        operator mysqlxx::Connection & () &
        {
            forceConnected();
            return data->conn;
        }

        operator const mysqlxx::Connection & () const &
        {
            forceConnected();
            return data->conn;
        }

        const mysqlxx::Connection * operator->() const &
        {
            forceConnected();
            return &data->conn;
        }

        mysqlxx::Connection * operator->() &
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

        void disconnect();

        friend class Pool;

    private:
        /// Pointer to mysqlxx connection.
        Connection * data = nullptr;
        /// Pointer to pool we are belonging to.
        Pool * pool = nullptr;

        /// Connects to database. If connection is failed then waits and repeats again.
        void forceConnected() const;

        /// Connects to database. If connection is failed then returns false.
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
     * @param config_name             Setting name in configuration file
     * @param default_connections_    Number of default connections
     * @param max_connections_        Maximum number of connections
     */
    Pool(const Poco::Util::AbstractConfiguration & cfg, const std::string & config_name,
         unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
         unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
         const char * parent_config_name_ = nullptr);

    /** Like with mysqlxx::Connection, either port either socket should be specified.
      * If server is localhost and socket is not empty, than socket is used. Otherwise, server and port is used.
      */
    Pool(const std::string & db_,
         const std::string & server_,
         const std::string & user_ = "",
         const std::string & password_ = "",
         unsigned port_ = 0,
         const std::string & socket_ = "",
         unsigned connect_timeout_ = MYSQLXX_DEFAULT_TIMEOUT,
         unsigned rw_timeout_ = MYSQLXX_DEFAULT_RW_TIMEOUT,
         unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
         unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
         unsigned enable_local_infile_ = MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE)
    : default_connections(default_connections_), max_connections(max_connections_),
    db(db_), server(server_), user(user_), password(password_), port(port_), socket(socket_),
    connect_timeout(connect_timeout_), rw_timeout(rw_timeout_), enable_local_infile(enable_local_infile_) {}

    Pool(const Pool & other)
        : default_connections{other.default_connections},
          max_connections{other.max_connections},
          db{other.db}, server{other.server},
          user{other.user}, password{other.password},
          port{other.port}, socket{other.socket},
          connect_timeout{other.connect_timeout}, rw_timeout{other.rw_timeout},
          enable_local_infile{other.enable_local_infile}
    {}

    Pool & operator=(const Pool &) = delete;

    ~Pool();

    /// Allocates connection.
    Entry Get();

    /// Allocates connection.
    /// If database is not accessible, returns empty Entry object.
    /// If pool is overflowed, throws exception.
    Entry tryGet();

    /// Get description of database.
    std::string getDescription() const
    {
        return description;
    }

protected:
    /// Number of MySQL connections which are created at launch.
    unsigned default_connections;
    /// Maximum possible number of connections
    unsigned max_connections;

private:
    /// Initialization flag.
    bool initialized{false};
    /// List of connections.
    using Connections = std::list<Connection *>;
    /// List of connections.
    Connections connections;
    /// Lock for connections list access
    std::mutex mutex;
    /// Description of connection.
    std::string description;

    /// Connection settings.
    std::string db;
    std::string server;
    std::string user;
    std::string password;
    unsigned port;
    std::string socket;
    unsigned connect_timeout;
    unsigned rw_timeout;
    std::string ssl_ca;
    std::string ssl_cert;
    std::string ssl_key;
    bool enable_local_infile;

    /// True if connection was established at least once.
    bool was_successful{false};

    /// Initialises class if it wasn't.
    void initialize();

    /** Create new connection. */
    Connection * allocConnection(bool dont_throw_if_failed_first_time = false);
};

}
