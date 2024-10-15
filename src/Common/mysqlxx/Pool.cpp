#if __has_include(<mysql.h>)
#include <mysql.h>
#include <mysqld_error.h>
#else
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#endif

#include <mysqlxx/Pool.h>
#include <base/sleep.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <ctime>


namespace mysqlxx
{

void Pool::Entry::incrementRefCount()
{
    if (!data)
        return;

    /// First reference, initialize thread
    if (data->ref_count.fetch_add(1) == 0)
        mysql_thread_init();
}


void Pool::Entry::decrementRefCount()
{
    if (!data)
        return;

    const auto ref_count = data->ref_count.fetch_sub(1);
    if (ref_count == 1)
    {
        /// We were the last user of this thread, deinitialize it
        mysql_thread_end();
        /// In Pool::Entry::disconnect() we remove connection from the list of pool's connections.
        /// So now we must deallocate the memory.
        if (data->removed_from_pool)
        {
            data->conn.disconnect();
            ::delete data;
        }
    }
}


Pool::Pool(const Poco::Util::AbstractConfiguration & cfg, const std::string & config_name,
     unsigned default_connections_, unsigned max_connections_,
     const char * parent_config_name_)
    : default_connections(default_connections_)
    , max_connections(max_connections_)
{
    server = cfg.getString(config_name + ".host");

    if (parent_config_name_)
    {
        const std::string parent_config_name(parent_config_name_);
        db = cfg.getString(config_name + ".db", cfg.getString(parent_config_name + ".db", ""));
        user = cfg.has(config_name + ".user")
            ? cfg.getString(config_name + ".user")
            : cfg.getString(parent_config_name + ".user");
        password = cfg.has(config_name + ".password")
            ? cfg.getString(config_name + ".password")
            : cfg.getString(parent_config_name + ".password");

        if (!cfg.has(config_name + ".port") && !cfg.has(config_name + ".socket")
            && !cfg.has(parent_config_name + ".port") && !cfg.has(parent_config_name + ".socket"))
            throw Poco::Exception("mysqlxx::Pool configuration: expected port or socket");

        port = cfg.has(config_name + ".port")
            ? cfg.getInt(config_name + ".port")
            : cfg.getInt(parent_config_name + ".port", 0);
        socket = cfg.has(config_name + ".socket")
            ? cfg.getString(config_name + ".socket")
            : cfg.getString(parent_config_name + ".socket", "");
        ssl_ca = cfg.has(config_name + ".ssl_ca")
            ? cfg.getString(config_name + ".ssl_ca")
            : cfg.getString(parent_config_name + ".ssl_ca", "");
        ssl_cert = cfg.has(config_name + ".ssl_cert")
            ? cfg.getString(config_name + ".ssl_cert")
            : cfg.getString(parent_config_name + ".ssl_cert", "");
        ssl_key = cfg.has(config_name + ".ssl_key")
            ? cfg.getString(config_name + ".ssl_key")
            : cfg.getString(parent_config_name + ".ssl_key", "");

        enable_local_infile = cfg.getBool(config_name + ".enable_local_infile",
            cfg.getBool(parent_config_name + ".enable_local_infile", MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE));

        opt_reconnect = cfg.getBool(config_name + ".opt_reconnect",
            cfg.getBool(parent_config_name + ".opt_reconnect", MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT));
    }
    else
    {
        db = cfg.getString(config_name + ".db", "");
        user = cfg.getString(config_name + ".user");
        password = cfg.getString(config_name + ".password");

        if (!cfg.has(config_name + ".port") && !cfg.has(config_name + ".socket"))
            throw Poco::Exception("mysqlxx::Pool configuration: expected port or socket");

        port = cfg.getInt(config_name + ".port", 0);
        socket = cfg.getString(config_name + ".socket", "");
        ssl_ca = cfg.getString(config_name + ".ssl_ca", "");
        ssl_cert = cfg.getString(config_name + ".ssl_cert", "");
        ssl_key = cfg.getString(config_name + ".ssl_key", "");

        enable_local_infile = cfg.getBool(
            config_name + ".enable_local_infile", MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE);

        opt_reconnect = cfg.getBool(config_name + ".opt_reconnect", MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT);
    }

    connect_timeout = cfg.getInt(config_name + ".connect_timeout",
        cfg.getInt("mysql_connect_timeout",
            MYSQLXX_DEFAULT_TIMEOUT));

    rw_timeout =
        cfg.getInt(config_name + ".rw_timeout",
            cfg.getInt("mysql_rw_timeout",
                MYSQLXX_DEFAULT_RW_TIMEOUT));
}


Pool::Pool(
    const std::string & db_,
     const std::string & server_,
     const std::string & user_,
     const std::string & password_,
     unsigned port_,
     const std::string & socket_,
     unsigned connect_timeout_,
     unsigned rw_timeout_,
     unsigned default_connections_,
     unsigned max_connections_,
     unsigned enable_local_infile_,
     bool opt_reconnect_)
    : default_connections(default_connections_)
    , max_connections(max_connections_)
    , db(db_)
    , server(server_)
    , user(user_)
    , password(password_)
    , port(port_)
    , socket(socket_)
    , connect_timeout(connect_timeout_)
    , rw_timeout(rw_timeout_)
    , enable_local_infile(enable_local_infile_)
    , opt_reconnect(opt_reconnect_)
{
    LOG_DEBUG(log,
        "Created MySQL Pool with settings: connect_timeout={}, read_write_timeout={}, default_connections_number={}, max_connections_number={}",
        connect_timeout, rw_timeout, default_connections, max_connections);
}


Pool::~Pool()
{
    std::lock_guard lock(mutex);

    for (auto & connection : connections)
        delete static_cast<Connection *>(connection);
}


Pool::Entry Pool::get(uint64_t wait_timeout)
{
    std::unique_lock<std::mutex> lock(mutex);

    uint64_t deadline = 0;
    /// UINT64_MAX -- wait indefinitely
    if (wait_timeout && wait_timeout != UINT64_MAX)
        deadline = clock_gettime_ns() + wait_timeout * 1'000'000'000;

    initialize();
    for (;;)
    {
        LOG_TRACE(log, "{}: Iterating through existing MySQL connections", getDescription());

        for (auto & connection : connections)
        {
            if (connection->ref_count == 0)
            {
                LOG_TEST(log, "Found free connection in pool, returning it to the caller");
                return Entry(connection, this);
            }
        }

        LOG_TRACE(log, "{}: Trying to allocate a new connection.", getDescription());
        if (connections.size() < static_cast<size_t>(max_connections))
        {
            Connection * conn = allocConnection();
            if (conn)
                return Entry(conn, this);

            LOG_TRACE(log, "{}: Unable to create a new connection: Allocation failed.", getDescription());
        }
        else
        {
            LOG_TRACE(log, "{}: Unable to create a new connection: Max number of connections has been reached.", getDescription());
        }

        if (!wait_timeout)
            throw Poco::Exception("mysqlxx::Pool is full (wait is disabled, see connection_wait_timeout setting)");

        if (deadline && clock_gettime_ns() >= deadline)
            throw Poco::Exception("mysqlxx::Pool is full (connection_wait_timeout is exceeded)");

        lock.unlock();
        LOG_TRACE(log, "{}: Sleeping for {} seconds.", getDescription(), MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);
        sleepForSeconds(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);
        lock.lock();
    }
}


Pool::Entry Pool::tryGet()
{
    std::lock_guard lock(mutex);

    initialize();

    /// Try to pick an idle connection from already allocated
    for (auto connection_it = connections.cbegin(); connection_it != connections.cend();)
    {
        Connection * connection_ptr = *connection_it;
        if (connection_ptr->ref_count == 0)
        {
            {
                Entry res(connection_ptr, this);
                if (res.tryForceConnected())  /// Tries to reestablish connection as well
                    return res;
            }

            LOG_DEBUG(log, "{}: Idle connection to MySQL server cannot be recovered, dropping it.", getDescription());

            /// This one is disconnected, cannot be reestablished and so needs to be disposed of.
            connection_it = connections.erase(connection_it);
            ::delete connection_ptr;  /// TODO: Manual memory management is awkward (matches allocConnection() method)
        }
        else
            ++connection_it;
    }

    if (connections.size() >= max_connections)
        throw Poco::Exception("mysqlxx::Pool is full");

    Connection * connection_ptr = allocConnection(true);
    if (connection_ptr)
        return {connection_ptr, this};

    return {};
}


void Pool::removeConnection(Connection* connection)
{
    LOG_TRACE(log, "{}: Removing connection.", getDescription());

    std::lock_guard lock(mutex);
    if (connection)
    {
        connections.remove(connection);
        connection->removed_from_pool = true;
    }
}


void Pool::Entry::disconnect()
{
    // Remove the Entry from the Pool. Actual disconnection is delayed until refcount == 0.
    pool->removeConnection(data);
}


void Pool::Entry::forceConnected() const
{
    if (data == nullptr)
        throw Poco::RuntimeException("Tried to access NULL database connection.");

    bool first = true;
    while (!tryForceConnected())
    {
        if (first)
            first = false;
        else
            sleepForSeconds(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);

        LOG_DEBUG(pool->log,
            "Creating a new MySQL connection to {} with settings: connect_timeout={}, read_write_timeout={}",
            pool->description, pool->connect_timeout, pool->rw_timeout);

        data->conn.connect(
            pool->db.c_str(),
            pool->server.c_str(),
            pool->user.c_str(),
            pool->password.c_str(),
            pool->port,
            pool->socket.c_str(),
            pool->ssl_ca.c_str(),
            pool->ssl_cert.c_str(),
            pool->ssl_key.c_str(),
            pool->connect_timeout,
            pool->rw_timeout,
            pool->enable_local_infile,
            pool->opt_reconnect);
    }
}


bool Pool::Entry::tryForceConnected() const
{
    auto * const mysql_driver = data->conn.getDriver();
    const auto prev_connection_id = mysql_thread_id(mysql_driver);

    LOG_TRACE(pool->log, "Entry(connection {}): sending PING to check if it is alive.", prev_connection_id);
    if (data->conn.ping())  /// Attempts to reestablish lost connection
    {
        const auto current_connection_id = mysql_thread_id(mysql_driver);
        if (prev_connection_id != current_connection_id)
        {
            LOG_DEBUG(pool->log, "Entry(connection {}): Reconnected to MySQL server. Connection id changed: {} -> {}",
                current_connection_id, prev_connection_id, current_connection_id);
        }

        LOG_TRACE(pool->log, "Entry(connection {}): PING ok.", current_connection_id);
        return true;
    }

    LOG_TRACE(pool->log, "Entry(connection {}): PING failed.", prev_connection_id);
    return false;
}


void Pool::initialize()
{
    if (!initialized)
    {
        description = db + "@" + server + ":" + std::to_string(port) + " as user " + user;

        for (unsigned i = 0; i < default_connections; ++i)
            allocConnection();

        initialized = true;
    }
}


Pool::Connection * Pool::allocConnection(bool dont_throw_if_failed_first_time)
{
    std::unique_ptr conn_ptr = std::make_unique<Connection>();

    try
    {
        LOG_DEBUG(log, "Connecting to {}", description);

        LOG_DEBUG(log,
            "Creating a new MySQL connection to {} with settings: connect_timeout={}, read_write_timeout={}",
            description, connect_timeout, rw_timeout);

        conn_ptr->conn.connect(
            db.c_str(),
            server.c_str(),
            user.c_str(),
            password.c_str(),
            port,
            socket.c_str(),
            ssl_ca.c_str(),
            ssl_cert.c_str(),
            ssl_key.c_str(),
            connect_timeout,
            rw_timeout,
            enable_local_infile,
            opt_reconnect);
    }
    catch (mysqlxx::ConnectionFailed & e)
    {
        LOG_ERROR(log, "Failed to connect to MySQL ({}): {}", description, e.what());

        if ((!was_successful && !dont_throw_if_failed_first_time)
            || e.errnum() == ER_ACCESS_DENIED_ERROR
            || e.errnum() == ER_DBACCESS_DENIED_ERROR
            || e.errnum() == ER_BAD_DB_ERROR)
        {
            throw;
        }

        return nullptr;
    }

    connections.push_back(conn_ptr.get());
    was_successful = true;
    return conn_ptr.release();
}

}
