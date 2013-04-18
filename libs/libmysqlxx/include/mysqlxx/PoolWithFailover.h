#pragma once

#include "Pool.h"


#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS	1
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS		16
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES			3


namespace mysqlxx
{
	
	/** Пул соединений с MySQL.
	  * Знает о наборе реплик с приоритетами.
	  * Пробует соединяться с репликами в порядке приоритета. При равном приоритете предпочитается реплика, к которой дольше всего не было попытки подключения.
	  * 
	  * Использование аналогично mysqlxx::Pool. В конфиге задание сервера может выглядеть так же, как для Pool:
	  * <mysql_metrica>
	  * 	<host>mtstat01c*</host>
	  * 	<port>3306</port>
	  * 	<user>metrica</user>
	  * 	<password></password>
	  * 	<db>Metrica</db>
	  * </mysql_metrica>
	  * 
	  * или так:
	  * 
	  * <mysql_metrica>
	  * 	<replica>
	  * 		<host>mtstat01c</host>
	  * 		<port>3306</port>
	  * 		<user>metrica</user>
	  * 		<password></password>
	  * 		<db>Metrica</db>
	  * 		<priority>0</priority>
	  * 	</replica>
	  * 	<replica>
	  * 		<host>mtstat01d</host>
	  * 		<port>3306</port>
	  * 		<user>metrica</user>
	  * 		<password></password>
	  * 		<db>Metrica</db>
	  * 		<priority>1</priority>
	  * 	</replica>
	  * </mysql_metrica>
	  */
	class PoolWithFailover
	{
	private:
		typedef Poco::SharedPtr<Pool> PoolPtr;
		
		struct Replica
		{
			PoolPtr pool;
			int priority;
			int error_count;
			
			Replica() : priority(0), error_count(0) {}
			Replica(PoolPtr pool_, int priority_)
				: pool(pool_), priority(priority_), error_count(0) {}
		};
		
		typedef std::vector<Replica> Replicas;
		/// [приоритет][номер] -> реплика.
		typedef std::map<int, Replicas> ReplicasByPriority;
		
		ReplicasByPriority replicas_by_priority;
		
		/// Количество попыток подключения.
		size_t max_tries;
		/// Mutex для доступа к списку реплик.
		Poco::FastMutex mutex;
		
	public:
		typedef Pool::Entry Entry;
		
		/**
		 * @param config_name		Имя параметра в конфигурационном файле.
		 * @param default_connections	Количество подключений по умолчанию к какждой реплике.
		 * @param max_connections	Максимальное количество подключений к какждой реплике.
		 * @param max_tries_		Количество попыток подключения.
		 */
		PoolWithFailover(const std::string & config_name,
			unsigned default_connections = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
			unsigned max_connections = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
			size_t max_tries_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES)
			: max_tries(max_tries_)
		{
			Poco::Util::Application & app = Poco::Util::Application::instance();
			Poco::Util::AbstractConfiguration & cfg = app.config();
			
			if (cfg.has(config_name + ".replica"))
			{
				Poco::Util::AbstractConfiguration::Keys replica_keys;
				cfg.keys(config_name, replica_keys);
				for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = replica_keys.begin(); it != replica_keys.end(); ++it)
				{
					if (it->size() < std::string("replica").size() || it->substr(0, std::string("replica").size()) != "replica")
						throw Poco::Exception("Unknown element in config: " + *it + ", expected replica");
					std::string replica_name = config_name + "." + *it;
					Replica replica(new Pool(replica_name, default_connections, max_connections), cfg.getInt(replica_name + ".priority", 0));
					replicas_by_priority[replica.priority].push_back(replica);
				}
			}
			else
			{
				replicas_by_priority[0].push_back(Replica(new Pool(config_name, default_connections, max_connections), 0));
			}
		}
		
		/** Выделяет соединение для работы. */
		Entry Get()
		{
			Poco::ScopedLock<Poco::FastMutex> locker(mutex);
			Poco::Util::Application & app = Poco::Util::Application::instance();
			
			/// Если к какой-то реплике не подключились, потому что исчерпан лимит соединений, можно подождать и подключиться к ней.
			Replica * full_pool = NULL;
			
			for (size_t try_no = 0; try_no < max_tries; ++try_no)
			{
				full_pool = NULL;
				
				for (ReplicasByPriority::reverse_iterator it = replicas_by_priority.rbegin(); it != replicas_by_priority.rend(); ++it)
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
						catch (Poco::Exception & e)
						{
							if (e.displayText() == "mysqlxx::Pool is full")
							{
								full_pool = &replica;
							}
							
							app.logger().error("Connection to " + replica.pool->getDescription() + " failed: " + e.displayText());
						}
					}
				}
				
				app.logger().error("Connection to all replicas failed " + Poco::NumberFormatter::format(try_no + 1) + " times");
			}
			
			if (full_pool)
			{
				app.logger().error("All connections failed, trying to wait on a full pool " + full_pool->pool->getDescription());
				return full_pool->pool->Get();
			}
			
			throw Poco::Exception("Connections to all replicas failed");
		}
	};
}
