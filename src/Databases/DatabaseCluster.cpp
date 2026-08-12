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
    ClusterPtr current = getContext()->getCluster(getContext()->getMacros()->expand(cluster_name));

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
    auto storage = fetchTable(table_name, local_context, throw_on_error);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist on the cluster", remote_database, table_name);
        return nullptr;
    }

    /// The table is exposed as the `Distributed` table engine over the named cluster, which is the
    /// persistent counterpart of the `cluster` table function. Unlike the `Remote` database engine,
    /// whose live proxy can be bound to an ad-hoc fallback cluster whose addresses have to be
    /// serialized explicitly (see `DatabaseRemote::getCreateTableQueryImpl`), the emitted definition
    /// always carries the configured cluster name: the remote-only fallback subset has no name of
    /// its own, and a `Distributed` table recreated from the definition performs the same
    /// local-replica fallback when reading anyway.
    auto table_storage_define = database_engine_define->clone();
    {
        ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
        ast_storage->engine->name = "Distributed";
        ast_storage->engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
        ast_storage->reset(ast_storage->settings);

        /// A proxy over more than one shard carries an implicit `rand()` sharding key (see
        /// `DatabaseRemote::fetchTable`). The key has to be serialized explicitly: otherwise the
        /// emitted definition recreates a table that rejects a multi-shard `INSERT`
        /// (`STORAGE_REQUIRES_PARAMETER`), while the live proxy accepts it.
        const auto * distributed = typeid_cast<const StorageDistributed *>(storage.get());
        const bool has_implicit_sharding_key = distributed && distributed->getCluster()->getShardsInfo().size() > 1;

        auto & engine_arguments = ast_storage->engine->arguments->children;
        engine_arguments = {
            make_intrusive<ASTLiteral>(cluster_name),
            make_intrusive<ASTLiteral>(remote_database),
            make_intrusive<ASTLiteral>(table_name)};
        if (has_implicit_sharding_key)
            engine_arguments.push_back(makeASTFunction("rand"));
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

        if (!engine->arguments || engine->arguments->children.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Engine `{}` requires 2 arguments: 'cluster_name', 'database'", engine_name);

        ASTs & engine_args = engine->arguments->children;

        /// The cluster name is usually written as an identifier, like in the `Distributed` table
        /// engine; the helper also supports quoted literals and names with hyphens.
        const String cluster_name = getClusterNameAndMakeLiteral(engine_args[0]);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.context);
        const String remote_database = safeGetLiteralValue<String>(engine_args[1], engine_name);

        /// A mistyped cluster name must fail the `CREATE DATABASE` right away. On attach (e.g. on
        /// server startup) the check is skipped: the cluster is resolved on every access anyway, and
        /// a database whose cluster has disappeared from the configuration must not prevent the
        /// server from starting.
        if (args.mode == LoadingStrictnessLevel::CREATE)
            args.context->getCluster(args.context->getMacros()->expand(cluster_name));

        return std::make_shared<DatabaseCluster>(
            args.context,
            args.metadata_path,
            engine_define,
            args.database_name,
            cluster_name,
            remote_database,
            args.uuid);
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
