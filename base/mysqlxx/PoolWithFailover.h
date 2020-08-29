#pragma once

#include "Pool.h"


#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS 1
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS 16
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3


namespace mysqlxx
{
    /** MySQL connection pool with support of failover.
      * Have information about replicas and their priorities.
      * Tries to connect to replica in an order of priority. When equal priority, choose replica with maximum time without connections.
      *
      * It could be configured without replicas, exactly as ordinary Pool:
      *
      * <mysql_metrica>
      *     <host>mtstat01c*</host>
      *     <port>3306</port>
      *     <user>metrica</user>
      *     <password></password>
      *     <db>Metrica</db>
      * </mysql_metrica>
      *
      * Or like this:
      *
      * <mysql_metrica>
      *     <replica>
      *         <host>mtstat01c</host>
      *         <port>3306</port>
      *         <user>metrica</user>
      *         <password></password>
      *         <db>Metrica</db>
      *         <priority>0</priority>
      *     </replica>
      *     <replica>
      *         <host>mtstat01d</host>
      *         <port>3306</port>
      *         <user>metrica</user>
      *         <password></password>
      *         <db>Metrica</db>
      *         <priority>1</priority>
      *     </replica>
      * </mysql_metrica>
      *
      * Or like this:
      *
      *  <mysql_metrica>
      *     <port>3306</port>
      *     <user>metrica</user>
      *     <password></password>
      *     <db>Metrica</db>
      *     <replica>
      *         <host>mtstat01c</host>
      *         <priority>0</priority>
      *     </replica>
      *     <replica>
      *         <host>mtstat01d</host>
      *         <priority>1</priority>
      *     </replica>
      * </mysql_metrica>
      */
    class PoolWithFailover final
    {
    private:
        using PoolPtr = std::shared_ptr<Pool>;
        using Replicas = std::vector<PoolPtr>;

        /// [priority][index] -> replica.
        using ReplicasByPriority = std::map<int, Replicas>;

        ReplicasByPriority replicas_by_priority;

        /// Number of connection tries.
        size_t max_tries;
        /// Mutex for set of replicas.
        std::mutex mutex;

        /// Can the Pool be shared
        bool shareable;

    public:
        using Entry = Pool::Entry;

        /**
         * config_name           Name of parameter in configuration file.
         * default_connections   Number of connection in pool to each replica at start.
         * max_connections       Maximum number of connections in pool to each replica.
         * max_tries_            Max number of connection tries.
         */
        PoolWithFailover(const std::string & config_name_,
            unsigned default_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
            unsigned max_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
            size_t max_tries_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

        PoolWithFailover(const Poco::Util::AbstractConfiguration & config_,
            const std::string & config_name_,
            unsigned default_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
            unsigned max_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
            size_t max_tries_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

        PoolWithFailover(const PoolWithFailover & other);

        /** Allocates a connection to use. */
        Entry get();
    };
}
