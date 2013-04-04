#pragma once

#include "Pool.h"


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
		
		/// Максимально возможное количество соедиений с каждой репликой.
		unsigned max_connections;
		/// Mutex для доступа к списку реплик.
		Poco::FastMutex mutex;
		
	public:
		typedef Pool::Entry Entry;
		
		/**
		 * @param config_name		Имя параметра в конфигурационном файле.
		 * @param max_connections_	Максимальное количество подключений к какждой реплике
		 */
		PoolWithFailover(const std::string & config_name,
			unsigned max_connections_ = MYSQLXX_POOL_DEFAULT_MAX_CONNECTIONS)
			: max_connections(max_connections_)
		{
			Poco::Util::Application & app = Poco::Util::Application::instance();
			Poco::Util::AbstractConfiguration & cfg = app.config();
			
			if (cfg.has(config_name + ".replica"))
			{
				Poco::Util::AbstractConfiguration::Keys replica_keys;
				cfg.keys(config_name, replica_keys);
				std::map<int, Replicas> replicas_by_priority;
				for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = replica_keys.begin(); it != replica_keys.end(); ++it)
				{
					if (it->size() < std::string("replica").size() || it->substr(0, std::string("replica").size()) != "replica")
						throw Poco::Exception("Unknown element in config: " + *it + ", expected replica");
					std::string replica_name = config_name + "." + *it;
					Replica replica(new Pool(replica_name, 0, max_connections), cfg.getInt(replica_name + ".priority", 0));
					replicas_by_priority[replica.priority].push_back(replica);
				}
			}
			else
			{
				replicas_by_priority[0].push_back(Replica(new Pool(config_name, 0, max_connections), 0));
			}
		}
		
		/** Выделяет соединение для работы. */
		Entry Get()
		{
			Poco::ScopedLock<Poco::FastMutex> locker(mutex);
			Poco::Util::Application & app = Poco::Util::Application::instance();
			
			/// Если к какой-то реплике не подключились, потому что исчерпан лимит соединений, можно подождать и подключиться к ней.
			Replica * full_pool;
			
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
			
			if (full_pool)
			{
				app.logger().error("All connections failed, trying to wait on a full pool " + full_pool->pool->getDescription());
				return full_pool->pool->Get();
			}
			
			throw Poco::Exception("Connections to all replicas failed");
		}
	};
}
