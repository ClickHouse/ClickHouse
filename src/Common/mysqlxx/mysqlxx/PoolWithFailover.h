#pragma once

#include <mysqlxx/Pool.h>


/// NOLINTBEGIN(modernize-macro-to-enum)
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS 1
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS 16
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3
#define MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_CONNECTION_WAIT_TIMEOUT 5 /// in seconds
/// NOLINTEND(modernize-macro-to-enum)


namespace mysqlxx
{
    /** MySQL connection pool with support of failover.
      *
      * For dictionary source:
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

        /// [priority][index] -> replica. Highest priority is 0.
        using ReplicasByPriority = std::map<int, Replicas>;
        ReplicasByPriority replicas_by_priority;

        /// Number of connection tries.
        size_t max_tries;
        /// Mutex for set of replicas.
        std::mutex mutex;
        /// Can the Pool be shared
        bool shareable;
        /// Timeout for waiting free connection.
        uint64_t wait_timeout = 0;
        /// Attempt to reconnect in background thread
        bool bg_reconnect = false;

    public:
        using Entry = Pool::Entry;
        using RemoteDescription = std::vector<std::pair<std::string, uint16_t>>;

        /**
         * * Mysql dictionary sourse related params:
         * config_name           Name of parameter in configuration file for dictionary source.
         *
         * * Mysql storage related parameters:
         * replicas_description
         *
         * * Mutual parameters:
         * default_connections   Number of connection in pool to each replica at start.
         * max_connections       Maximum number of connections in pool to each replica.
         * max_tries_            Max number of connection tries.
         * wait_timeout_         Timeout for waiting free connection.
         */
        explicit PoolWithFailover(
            const std::string & config_name_,
            unsigned default_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
            unsigned max_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
            size_t max_tries_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

        PoolWithFailover(
            const Poco::Util::AbstractConfiguration & config_,
            const std::string & config_name_,
            unsigned default_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
            unsigned max_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
            size_t max_tries_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES);

        PoolWithFailover(
            const std::string & database,
            const RemoteDescription & addresses,
            const std::string & user,
            const std::string & password,
            const std::string & ssl_ca,
            const std::string & ssl_cert,
            const std::string & ssl_key,
            unsigned default_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
            unsigned max_connections_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_CONNECTIONS,
            size_t max_tries_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
            uint64_t wait_timeout_ = MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_CONNECTION_WAIT_TIMEOUT,
            size_t connect_timeout = MYSQLXX_DEFAULT_TIMEOUT,
            size_t rw_timeout = MYSQLXX_DEFAULT_RW_TIMEOUT,
            bool bg_reconnect_ = false);

        PoolWithFailover(const PoolWithFailover & other);

        /** Allocates a connection to use. */
        Entry get();
    };

    using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;
}
