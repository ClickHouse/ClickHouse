#include <mysqlxx/PoolWithFailover.h>

using namespace mysqlxx;

PoolWithFailover::PoolWithFailover(const std::string & config_name, unsigned default_connections, 
								   unsigned max_connections, size_t max_tries_)
	: max_tries(max_tries_)
{
	Poco::Util::Application & app = Poco::Util::Application::instance();
	Poco::Util::AbstractConfiguration & cfg = app.config();
	
	if (cfg.has(config_name + ".replica"))
	{
		int port_g = 0;
		std::string user_g;
		std::string password_g;
		std::string db_g;
		
		if (cfg.has(config_name + ".user"))
			user_g = cfg.getString(config_name + ".user");
		if (cfg.has(config_name + ".password"))
			password_g = cfg.getString(config_name + ".password");
		if (cfg.has(config_name + ".db"))
			db_g= cfg.getString(config_name + ".db");
		if (cfg.has(config_name + ".port"))
			port_g = cfg.getInt(config_name + ".port");
		
		Poco::Util::AbstractConfiguration::Keys replica_keys;
		cfg.keys(config_name, replica_keys);
		for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = replica_keys.begin(); it != replica_keys.end(); ++it)
		{
			if (!(*it == "port" || *it == "user" || *it == "password" || *it == "db"))
			{
				if (it->size() < std::string("replica").size() || it->substr(0, std::string("replica").size()) != "replica")
					throw Poco::Exception("Unknown element in config: " + *it + ", expected replica");
				std::string replica_name = config_name + "." + *it;
				Replica replica(new Pool(replica_name, default_connections, max_connections, user_g, password_g, db_g, port_g),
								cfg.getInt(replica_name + ".priority", 0));
				replicas_by_priority[replica.priority].push_back(replica);
			}
		}
	}
	else
	{
		replicas_by_priority[0].push_back(Replica(new Pool(config_name, default_connections, max_connections), 0));
	}
}

PoolWithFailover::Entry PoolWithFailover::Get()
{
	Poco::ScopedLock<Poco::FastMutex> locker(mutex);
	Poco::Util::Application & app = Poco::Util::Application::instance();

	/// Если к какой-то реплике не подключились, потому что исчерпан лимит соединений, можно подождать и подключиться к ней.
	Replica * full_pool = NULL;

	for (size_t try_no = 0; try_no < max_tries; ++try_no)
	{
		full_pool = NULL;

		for (ReplicasByPriority::iterator it = replicas_by_priority.begin(); it != replicas_by_priority.end(); ++it)
		{
			Replicas & replicas = it->second;
			for (size_t i = 0; i < replicas.size(); ++i)
			{
				Replica & replica = replicas[i];

				try
				{
					Entry entry = replica.pool->tryGet();

					if (!entry.isNull())
					{
						/// Переместим все пройденные реплики в конец очереди.
						/// Пройденные реплики с другим приоритетом перемещать незачем.
						std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());

						return entry;
					}
				}
				catch (const Poco::Exception & e)
				{
					if (e.displayText() == "mysqlxx::Pool is full")
					{
						full_pool = &replica;
					}

					app.logger().warning("Connection to " + replica.pool->getDescription() + " failed: " + e.displayText());
					continue;
				}

				app.logger().warning("Connection to " + replica.pool->getDescription() + " failed.");
			}
		}

		app.logger().error("Connection to all replicas failed " + Poco::NumberFormatter::format(try_no + 1) + " times");
	}

	if (full_pool)
	{
		app.logger().error("All connections failed, trying to wait on a full pool " + full_pool->pool->getDescription());
		return full_pool->pool->Get();
	}

	std::stringstream message;
	message << "Connections to all replicas failed: ";
	for (ReplicasByPriority::const_iterator it = replicas_by_priority.begin(); it != replicas_by_priority.end(); ++it)
		for (Replicas::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt)
			message << (it == replicas_by_priority.begin() && jt == it->second.begin() ? "" : ", ") << jt->pool->getDescription();

	throw Poco::Exception(message.str());
}
