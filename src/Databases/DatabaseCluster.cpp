#include <Databases/DatabaseCluster.h>

#include <Core/Settings.h>
#include <Databases/DatabaseFactory.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageDistributed.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int THERE_IS_NO_QUERY;
    extern const int UNKNOWN_TABLE;
}


DatabaseCluster::DatabaseCluster(
    ContextPtr context_,
    const String & metadata_path_,
    const ASTStorage * database_engine_define_,
    const String & database_name_,
    const String & cluster_name_,
    const String & remote_database_,
    UUID uuid)
    : DatabaseRemote(
        context_,
        metadata_path_,
        database_engine_define_,
        database_name_,
        remote_database_,
        /* username_ = */ String{},
        /* password_ = */ String{},
        /* cluster_ = */ nullptr,
        /* remote_only_cluster_ = */ nullptr,
        /* secure_ = */ false,
        uuid)
    , cluster_name(cluster_name_)
{
    log = getLogger("DatabaseCluster(" + database_name_ + ")");
}


DatabaseRemote::ProxyClusters DatabaseCluster::getProxyClusters() const
{
    /// Macros (e.g. `{cluster}`) are expanded on every resolution: `StorageDistributed` expands them
    /// once per table object, but a database lives much longer than its per-lookup proxy tables, so
    /// the expansion has to follow the current configuration the same way the cluster itself does.
    ClusterPtr current = getContext()->getCluster(
        getContext()->getMacros()->expand(cluster_name),
        getContext()->getApplicationType() == Context::ApplicationType::LOCAL);

    std::lock_guard lock(cluster_cache_mutex);
    if (cached_cluster != current)
    {
        cached_remote_only_cluster = current->tryGetClusterWithoutLocalReplicas(getContext()->getSettingsRef());
        cached_cluster = current;
    }
    return {cached_cluster, cached_remote_only_cluster};
}


ASTPtr DatabaseCluster::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    const ProxyClusters clusters = getProxyClusters();
    auto storage = fetchTable(table_name, local_context, throw_on_error, clusters);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist on the cluster", remote_database, table_name);
        return nullptr;
    }

    /// The table is exposed as the `Distributed` table engine over the named cluster, which is the
    /// persistent counterpart of the `cluster` table function.
    const auto * distributed = typeid_cast<const StorageDistributed *>(storage.get());

    /// The live proxy is bound to the remote-only fallback cluster when the local replica of a
    /// shard does not have the database or the table (see `DatabaseRemote::fetchTableStructure`).
    /// A `Distributed` table over the whole named cluster is not equivalent to that object: it
    /// performs no such fallback on the metadata lookup, so replaying it fails on the missing
    /// local objects. The fallback subset has no name of its own either, and unlike the `Remote`
    /// engine (which serializes the effective fallback addresses together with its stored
    /// credentials, see `DatabaseRemote::getCreateTableQueryImpl`), the connections of this engine
    /// are defined by the per-replica settings of the cluster configuration, which no explicit
    /// address list can carry. No re-executable definition exists for this transient state, so
    /// report that instead of emitting a definition that recreates a different object.
    if (distributed && distributed->getCluster() != clusters.cluster)
    {
        if (throw_on_error)
            throw Exception(
                ErrorCodes::THERE_IS_NO_QUERY,
                "Table {}.{} is currently served through the remote-replica fallback (the local replica of a shard of cluster {} "
                "does not have the database or the table), and a `Distributed` table over the whole cluster would not perform "
                "this fallback, so there is no equivalent re-executable CREATE query for it",
                backQuoteIfNeed(remote_database),
                backQuoteIfNeed(table_name),
                backQuoteIfNeed(cluster_name));
        return nullptr;
    }

    /// A `Cluster` database expands macros on every access, whereas `StorageDistributed` expands
    /// its cluster name once when the recreated table is constructed. Serializing the original
    /// macro expression would therefore freeze a later configuration change, and serializing its
    /// current expansion would lose the per-access behavior. Neither is an equivalent definition.
    if (getContext()->getMacros()->expand(cluster_name) != cluster_name)
    {
        if (throw_on_error)
            throw Exception(
                ErrorCodes::THERE_IS_NO_QUERY,
                "Table {}.{} belongs to a `Cluster` database whose cluster name {} contains macros that are expanded on every access, "
                "but a `Distributed` table expands them only when it is created, so there is no equivalent re-executable CREATE query for "
                "it",
                backQuoteIfNeed(remote_database),
                backQuoteIfNeed(table_name),
                backQuoteIfNeed(cluster_name));
        return nullptr;
    }

    /// A `Cluster` database follows configuration reloads, including changes to the number of
    /// shards. A table emitted while the cluster has one shard would lack a sharding key, but
    /// after a reload that adds shards the live proxy acquires an implicit insert-only `rand()`
    /// key. Serializing that key unconditionally is not equivalent either: a standalone
    /// `Distributed` table uses it for read shard pruning. The key cannot be marked insert-only
    /// in a CREATE query, so no standalone table definition is equivalent to a `Cluster` proxy.
    if (distributed)
    {
        if (throw_on_error)
            throw Exception(
                ErrorCodes::THERE_IS_NO_QUERY,
                "Table {}.{} belongs to a reloadable `Cluster` database: a configuration reload can add shards and give the live proxy an "
                "implicit `rand()` sharding key used only for INSERT, but a standalone `Distributed` table would either lack that key or use it "
                "for read shard pruning, so there is no equivalent re-executable CREATE query for it",
                backQuoteIfNeed(remote_database),
                backQuoteIfNeed(table_name));
        return nullptr;
    }

    auto table_storage_define = database_engine_define->clone();
    {
        ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
        ast_storage->engine->name = "Distributed";
        ast_storage->engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
        ast_storage->reset(ast_storage->settings);

        auto & engine_arguments = ast_storage->engine->arguments->children;
        engine_arguments = {
            make_intrusive<ASTLiteral>(cluster_name),
            make_intrusive<ASTLiteral>(remote_database),
            make_intrusive<ASTLiteral>(table_name)};
    }

    /// Reuse the common serializer with `only_ordinary = false` so that column defaults, aliases and
    /// materialized expressions inferred from the remote table are preserved, exactly like the
    /// `Remote` database engine does.
    const Settings & settings = local_context->getSettingsRef();
    return getCreateQueryFromStorage(
        storage,
        table_storage_define,
        /* only_ordinary = */ false,
        static_cast<unsigned>(settings[Setting::max_parser_depth]),
        static_cast<unsigned>(settings[Setting::max_parser_backtracks]),
        throw_on_error,
        local_context);
}


void registerDatabaseCluster(DatabaseFactory & factory);
void registerDatabaseCluster(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args) -> DatabasePtr
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine->name;

        /// Server startup and other internal metadata replay attach a database from metadata that was
        /// already validated when it was created, and such a database must not prevent the server from
        /// starting; every user query, including an explicit `ATTACH DATABASE`, is validated in full.
        const bool is_metadata_replay = args.internal && args.mode >= LoadingStrictnessLevel::ATTACH;

        if (!engine->arguments || engine->arguments->children.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Engine `{}` requires 2 arguments: 'cluster_name', 'database'", engine_name);

        ASTs & engine_args = engine->arguments->children;

        /// The cluster name is usually written as an identifier, like in the `Distributed` table
        /// engine; the helper also supports quoted literals and names with hyphens.
        const String cluster_name = getClusterNameAndMakeLiteral(engine_args[0]);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.context);
        const String remote_database = safeGetLiteralValue<String>(engine_args[1], engine_name);

        /// A mistyped cluster name must fail the DDL query right away. On internal metadata replay
        /// (e.g. server startup) the check is skipped: the cluster is resolved on every access anyway,
        /// and a database whose cluster has disappeared from the configuration must not prevent the
        /// server from starting.
        if (!is_metadata_replay)
            args.context->getCluster(args.context->getMacros()->expand(cluster_name));

        auto database = std::make_shared<DatabaseCluster>(
            args.context,
            args.metadata_path,
            engine_define,
            args.database_name,
            cluster_name,
            remote_database,
            args.uuid);

        /// A chain of proxy databases on this server that refers back to itself is rejected eagerly
        /// (see `throwIfLocalChainRefersBack`), but not on internal metadata replay: a server that
        /// persisted such a chain (e.g. because a configuration reload closed the cycle) must still start.
        if (!is_metadata_replay)
            database->throwIfLocalChainRefersBack();

        return database;
    };

    factory.registerDatabase(
        "Cluster",
        create_fn,
        DatabaseFactory::EngineFeatures{
            .supports_arguments = true,
            .supports_settings = false,
            .is_external = true,
            .source_access_type = AccessTypeObjects::Source::REMOTE,
        },
        Documentation{
            .description = R"DOCS_MD(
The `Cluster` database engine provides real-time access to the tables of a database on a cluster from the server configuration. It is the named-cluster counterpart of the [`Remote`](/engines/database-engines/remote) database engine, exactly as the [`cluster`](/sql-reference/table-functions/cluster) table function relates to the [`remote`](/sql-reference/table-functions/remote) table function.

The list of tables and their structure are fetched from the cluster on demand, so the database always reflects its current state. Each table is exposed as a [`Distributed`](/engines/table-engines/special/distributed) storage over the named cluster, which forwards `SELECT` and `INSERT` queries to it. Connections use the per-replica settings of the cluster configuration (credentials, secure connections, compression, the inter-server secret), and the cluster definition is re-resolved on every access, so the database follows configuration reloads. When the cluster has several shards, an `INSERT` sends each row to a random shard (the proxy tables carry an implicit `rand()` sharding key, which only distributes the inserted rows and does not act as a shard-pruning key for reading); set `insert_shard_id` to pin the shard for a query.
)DOCS_MD",
            .syntax = "ENGINE = Cluster('cluster_name', 'database')",
            .related = {"Remote", "MySQL", "PostgreSQL"}});
}

}
