#include <mysqlxx/PoolWithFailover.h>


/// Duplicate of code from StringUtils.h. Copied here for less dependencies.
static bool startsWith(const std::string & s, const char * prefix)
{
    return s.size() >= strlen(prefix) && 0 == memcmp(s.data(), prefix, strlen(prefix));
}


using namespace mysqlxx;

PoolWithFailover::PoolWithFailover(const Poco::Util::AbstractConfiguration & cfg,
                                   const std::string & config_name, const unsigned default_connections,
                                   const unsigned max_connections, const size_t max_tries)
    : max_tries(max_tries)
{
    if (cfg.has(config_name + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys replica_keys;
        cfg.keys(config_name, replica_keys);
        for (const auto & replica_config_key : replica_keys)
        {
            /// There could be another elements in the same level in configuration file, like "password", "port"...
            if (startsWith(replica_config_key, "replica"))
            {
                std::string replica_name = config_name + "." + replica_config_key;

                int priority = cfg.getInt(replica_name + ".priority", 0);

                replicas_by_priority[priority].emplace_back(
                    std::make_shared<Pool>(cfg, replica_name, default_connections, max_connections, config_name.c_str()));
            }
        }
    }
    else
    {
        replicas_by_priority[0].emplace_back(
            std::make_shared<Pool>(cfg, config_name, default_connections, max_connections));
    }
}

PoolWithFailover::PoolWithFailover(const std::string & config_name, const unsigned default_connections,
    const unsigned max_connections, const size_t max_tries)
    : PoolWithFailover{
        Poco::Util::Application::instance().config(), config_name,
        default_connections, max_connections, max_tries}
{}

PoolWithFailover::PoolWithFailover(const PoolWithFailover & other)
    : max_tries{other.max_tries}
{
    for (const auto & priority_replicas : other.replicas_by_priority)
    {
        Replicas replicas;
        replicas.reserve(priority_replicas.second.size());
        for (const auto & pool : priority_replicas.second)
            replicas.emplace_back(std::make_shared<Pool>(*pool));
        replicas_by_priority.emplace(priority_replicas.first, std::move(replicas));
    }
}

PoolWithFailover::Entry PoolWithFailover::Get()
{
    Poco::Util::Application & app = Poco::Util::Application::instance();
    std::lock_guard<std::mutex> locker(mutex);

    /// If we cannot connect to some replica due to pool overflow, than we will wait and connect.
    PoolPtr * full_pool = nullptr;

    for (size_t try_no = 0; try_no < max_tries; ++try_no)
    {
        full_pool = nullptr;

        for (auto & priority_replicas : replicas_by_priority)
        {
            Replicas & replicas = priority_replicas.second;
            for (size_t i = 0, size = replicas.size(); i < size; ++i)
            {
                PoolPtr & pool = replicas[i];

                try
                {
                    Entry entry = pool->tryGet();

                    if (!entry.isNull())
                    {
                        /// Move all traversed replicas to the end of queue.
                        /// (No need to move replicas with another priority)
                        std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());

                        return entry;
                    }
                }
                catch (const Poco::Exception & e)
                {
                    if (e.displayText().find("mysqlxx::Pool is full") != std::string::npos) /// NOTE: String comparison is trashy code.
                    {
                        full_pool = &pool;
                    }

                    app.logger().warning("Connection to " + pool->getDescription() + " failed: " + e.displayText());
                    continue;
                }

                app.logger().warning("Connection to " + pool->getDescription() + " failed.");
            }
        }

        app.logger().error("Connection to all replicas failed " + std::to_string(try_no + 1) + " times");
    }

    if (full_pool)
    {
        app.logger().error("All connections failed, trying to wait on a full pool " + (*full_pool)->getDescription());
        return (*full_pool)->Get();
    }

    std::stringstream message;
    message << "Connections to all replicas failed: ";
    for (auto it = replicas_by_priority.begin(); it != replicas_by_priority.end(); ++it)
        for (auto jt = it->second.begin(); jt != it->second.end(); ++jt)
            message << (it == replicas_by_priority.begin() && jt == it->second.begin() ? "" : ", ") << (*jt)->getDescription();

    throw Poco::Exception(message.str());
}
