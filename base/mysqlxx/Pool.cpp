#if __has_include(<mysql.h>)
#include <mysql.h>
#include <mysqld_error.h>
#else
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#endif

#include <mysqlxx/Pool.h>

#include <common/sleep.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>


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

    /// We were the last user of this thread, deinitialize it
    if (data->ref_count.fetch_sub(1) == 1)
        mysql_thread_end();
}


Pool::Pool(const Poco::Util::AbstractConfiguration & cfg, const std::string & config_name,
     unsigned default_connections_, unsigned max_connections_,
     const char * parent_config_name_)
    : default_connections(default_connections_), max_connections(max_connections_)
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


Pool::~Pool()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (auto & connection : connections)
        delete static_cast<Connection *>(connection);
}


Pool::Entry Pool::get()
{
    std::unique_lock<std::mutex> lock(mutex);

    initialize();
    for (;;)
    {
        for (auto & connection : connections)
        {
            if (connection->ref_count == 0)
                return Entry(connection, this);
        }

        if (connections.size() < static_cast<size_t>(max_connections))
        {
            Connection * conn = allocConnection();
            if (conn)
                return Entry(conn, this);
        }

        lock.unlock();
        sleepForSeconds(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);
        lock.lock();
    }
}


Pool::Entry Pool::tryGet()
{
    std::lock_guard<std::mutex> lock(mutex);

    initialize();

    /// Try to pick an idle connection from already allocated
    for (auto connection_it = connections.cbegin(); connection_it != connections.cend();)
    {
        Connection * connection_ptr = *connection_it;
        /// Fixme: There is a race condition here b/c we do not synchronize with Pool::Entry's copy-assignment operator
        if (connection_ptr->ref_count == 0)
        {
            Entry res(connection_ptr, this);
            if (res.tryForceConnected())  /// Tries to reestablish connection as well
                return res;

            auto & logger = Poco::Util::Application::instance().logger();
            logger.information("Idle connection to mysql server cannot be recovered, dropping it.");

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
    std::lock_guard<std::mutex> lock(mutex);
    if (connection)
    {
        if (connection->ref_count > 0)
        {
            connection->conn.disconnect();
            connection->ref_count = 0;
        }
        connections.remove(connection);
    }
}


void Pool::Entry::disconnect()
{
    pool->removeConnection(data);
}


void Pool::Entry::forceConnected() const
{
    if (data == nullptr)
        throw Poco::RuntimeException("Tried to access NULL database connection.");

    Poco::Util::Application & app = Poco::Util::Application::instance();

    bool first = true;
    while (!tryForceConnected())
    {
        if (first)
            first = false;
        else
            sleepForSeconds(MYSQLXX_POOL_SLEEP_ON_CONNECT_FAIL);

        app.logger().information("MYSQL: Reconnecting to " + pool->description);
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
            pool->enable_local_infile);
    }
}


bool Pool::Entry::tryForceConnected() const
{
    auto * const mysql_driver = data->conn.getDriver();
    const auto prev_connection_id = mysql_thread_id(mysql_driver);
    if (data->conn.ping())  /// Attempts to reestablish lost connection
    {
        const auto current_connection_id = mysql_thread_id(mysql_driver);
        if (prev_connection_id != current_connection_id)
        {
            auto & logger = Poco::Util::Application::instance().logger();
            logger.information("Reconnected to mysql server. Connection id changed: %lu -> %lu",
                                prev_connection_id, current_connection_id);
        }
        return true;
    }

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
    Poco::Util::Application & app = Poco::Util::Application::instance();

    std::unique_ptr<Connection> conn(new Connection);

    try
    {
        app.logger().information("MYSQL: Connecting to " + description);

        conn->conn.connect(
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
            enable_local_infile);
    }
    catch (mysqlxx::ConnectionFailed & e)
    {
        if ((!was_successful && !dont_throw_if_failed_first_time)
            || e.errnum() == ER_ACCESS_DENIED_ERROR
            || e.errnum() == ER_DBACCESS_DENIED_ERROR
            || e.errnum() == ER_BAD_DB_ERROR)
        {
            app.logger().error(e.what());
            throw;
        }
        else
        {
            app.logger().error(e.what());
            return nullptr;
        }
    }

    was_successful = true;
    auto * connection = conn.release();
    connections.push_back(connection);
    return connection;
}

}
