#pragma once

#include <Common/Logger.h>

#include <list>
#include <memory>
#include <mutex>
#include <atomic>

#include <Poco/Exception.h>
#include <Poco/Logger.h>

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
 *            mysqlxx::Pool::Entry connection = pool.Get();
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
        /// Ref count modified in constructor/descructor of Entry
        /// but also read in pool code.
        std::atomic<int> ref_count = 0;
        std::atomic<bool> removed_from_pool = false;
    };

public:
    /// Connection with database.
    class Entry
    {
    public:
        Entry() {} /// NOLINT

        Entry(const Entry & src)
            : data(src.data), pool(src.pool)
        {
            incrementRefCount();
        }

        ~Entry()
        {
            decrementRefCount();
        }

        bool isNull() const
        {
            return data == nullptr;
        }

        operator mysqlxx::Connection & () & /// NOLINT
        {
            forceConnected();
            return data->conn;
        }

        operator const mysqlxx::Connection & () const & /// NOLINT
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
        bool tryForceConnected() const;

        void incrementRefCount();
        void decrementRefCount();
    };


    explicit Pool(const std::string & config_name,
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
         const std::string & user_,
         const std::string & password_,
         unsigned port_,
         const std::string & socket_ = "",
         unsigned connect_timeout_ = MYSQLXX_DEFAULT_TIMEOUT,
         unsigned rw_timeout_ = MYSQLXX_DEFAULT_RW_TIMEOUT,
         unsigned default_connections_ = MYSQLXX_POOL_DEFAULT_START_CONNECTIONS,
         unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS,
         unsigned enable_local_infile_ = MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE,
         bool opt_reconnect_ = MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT);

    Pool(const Pool & other)
        : default_connections{other.default_connections},
          max_connections{other.max_connections},
          db{other.db}, server{other.server},
          user{other.user}, password{other.password},
          port{other.port}, socket{other.socket},
          connect_timeout{other.connect_timeout}, rw_timeout{other.rw_timeout},
          enable_local_infile{other.enable_local_infile}, opt_reconnect(other.opt_reconnect)
    {}

    Pool & operator=(const Pool &) = delete;

    ~Pool();

    /// Allocates connection.
    Entry get(uint64_t wait_timeout = UINT64_MAX);

    /// Allocates connection.
    /// If database is not accessible, returns empty Entry object.
    /// If pool is overflowed, throws exception.
    Entry tryGet();

    /// Get description of database.
    std::string getDescription() const
    {
        return description;
    }

    void removeConnection(Connection * connection);

protected:
    LoggerPtr log = getLogger("mysqlxx::Pool");

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
    bool opt_reconnect;

    /// True if connection was established at least once.
    bool was_successful{false};

    /// Initialises class if it wasn't.
    void initialize();

    /** Create new connection. */
    Connection * allocConnection(bool dont_throw_if_failed_first_time = false);
};

}
