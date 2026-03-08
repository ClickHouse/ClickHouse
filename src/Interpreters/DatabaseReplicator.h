#pragma once

#include <Interpreters/DDLReplicator.h>
#include <Interpreters/DatabaseReplicatorSettings.h>
#include <Interpreters/QueryFlags.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class IAST;
class IDatabase;

/// Replicate DDLs for database (CREATE DATBASE, DROP DATABASE, ...) across cluster.
class DatabaseReplicator : boost::noncopyable, WithMutableContext, public DDLReplicator
{
public:
    DatabaseReplicator() = default;
    DatabaseReplicator(
        ContextMutablePtr context_,
        const String & zookeeper_name_,
        const String & zookeeper_path_,
        const String & shard_name_,
        const String & replica_name_,
        DatabaseReplicatorSettings settings_);

    String getName() const override;
    LoggerPtr getLogger() const override { return log; }

    static void init(ContextMutablePtr global_context_, const Poco::Util::AbstractConfiguration & config);
    static DatabaseReplicator & instance();
    static bool isEnabled();
    static void shutdown();

    void startup();

    /// Returns true if the given database should be replicated.
    /// Predefined databases (system, information_schema, etc.) and the default database are excluded.
    bool canReplicateDatabase(const String & database_name) const;

    /// Returns true if the given query should be replicated through the DatabaseReplicator DDL log.
    /// Only database-level DDL (CREATE DATABASE, DROP DATABASE) for replicable databases is accepted.
    /// Queries already marked as replicated-database-internal are not forwarded again.
    bool shouldReplicateQuery(const ContextPtr & query_context, const ASTPtr & query_ptr) const;

    /// Enqueue a replicated DDL query into the ZooKeeper log and wait for replicas.
    BlockIO tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context, QueryFlags flags);

    void commitCreateDatabase(const String & database_name, ContextPtr query_context);
    void commitDropDatabase(const String & database_name, ContextPtr query_context);
    void commitAlterDatabase(const String & database_name, ContextPtr query_context);
    void commitRenameDatabase(
        const String & database_name,
        const String & to_database_name,
        bool exchange,
        ContextPtr query_context);
private:
    const DDLReplicatorSettings & getSettings() const override;
    void tryConnectToZooKeeperAndInit();
    void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr) override;
    UInt64 getLocalDigest() const override;
    void initializeLocalDigest();
    String getCreateDatabaseStatement(const std::shared_ptr<IDatabase> & database);
    String getCreateDatabaseStatement(const String & database_name);
    ASTPtr parseQueryFromMetadataInZooKeeper(ContextPtr context_, const String & database_name, const String & query) const;

    static std::unique_ptr<DatabaseReplicator> database_replicator;

    /// A container to cache the local digests of the metadata files in memory.
    /// The key is the database name.
    /// It should be updated every time a database's DDL is modified.
    std::map<String, UInt64> local_digests TSA_GUARDED_BY(metadata_mutex);

    DatabaseReplicatorSettings settings;
    LoggerPtr log;
};

}
