#include <Databases/DatabaseRemote.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <Disks/IDisk.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/StorageDistributed.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Common/NetException.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>

#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 table_function_remote_max_addresses;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INFINITE_LOOP;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_REMOTE_SHARD_AVAILABLE;
    extern const int UNKNOWN_TABLE;
}


DatabaseRemote::DatabaseRemote(
    ContextPtr context_,
    const String & metadata_path_,
    const ASTStorage * database_engine_define_,
    const String & database_name_,
    const String & remote_database_,
    ClusterPtr cluster_,
    ClusterPtr remote_only_cluster_,
    bool secure_,
    UUID uuid)
    : DatabaseWithAltersOnDiskBase(database_name_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , remote_database(remote_database_)
    , cluster(std::move(cluster_))
    , remote_only_cluster(std::move(remote_only_cluster_))
    , secure(secure_)
    , log(getLogger("DatabaseRemote(" + database_name_ + ")"))
    , db_uuid(uuid)
{
    persistent = !context_->getClientInfo().is_shared_catalog_internal;
    if (persistent)
    {
        auto db_disk = getDisk();
        db_disk->createDirectories(metadata_path);
    }
}


namespace
{

/// Several `Remote` databases on this server may refer to each other in a cycle (e.g. `a` -> `b` -> `a`),
/// in which case following the local shard would recurse forever. The pointer-equality check of
/// `tryGetLocalDatabase` only catches the direct self-reference, so additionally track the databases
/// being traversed and reject re-entry. The traversal is synchronous, so a thread-local set suffices.
thread_local std::unordered_set<const IDatabase *> local_databases_in_traversal;

struct LocalTraversalGuard
{
    const IDatabase * database;

    explicit LocalTraversalGuard(const IDatabase * database_) : database(database_)
    {
        if (!local_databases_in_traversal.emplace(database).second)
            throw Exception(
                ErrorCodes::INFINITE_LOOP,
                "A chain of `Remote` databases containing {} refers to itself",
                backQuoteIfNeed(database->getDatabaseName()));
    }

    ~LocalTraversalGuard()
    {
        local_databases_in_traversal.erase(database);
    }
};

}


DatabasePtr DatabaseRemote::tryGetLocalDatabase() const
{
    auto local_database = DatabaseCatalog::instance().tryGetDatabase(remote_database);

    /// A database that refers to itself on the same server would recurse forever when its
    /// tables are listed, so reject it instead of hanging.
    if (local_database.get() == this)
        throw Exception(ErrorCodes::INFINITE_LOOP, "Database {} refers to itself", backQuoteIfNeed(getDatabaseName()));

    return local_database;
}


Strings DatabaseRemote::fetchTablesList(ContextPtr local_context) const
{
    auto sample_block = std::make_shared<const Block>(Block{
        {ColumnString::create(), std::make_shared<DataTypeString>(), "name"},
    });

    const String query = fmt::format("SELECT name FROM system.tables WHERE database = {}", quoteString(remote_database));

    /// This is a service query, not a real user query, so it must not fail because of the user's
    /// result-size limits (e.g. `max_result_rows` set for the current query).
    auto query_context = Context::createCopy(local_context);
    {
        Settings new_settings = query_context->getSettingsCopy();
        new_settings[Setting::max_result_rows] = 0;
        new_settings[Setting::max_result_bytes] = 0;
        query_context->setSettings(new_settings);
    }

    /// A shard that points to this server is a local shard (see `buildClusters`). Enumerate the
    /// local database under `local_context` instead of opening a self-connection with the stored
    /// engine credentials, mirroring the local-shard special case of `getStructureOfRemoteTable`.
    /// Otherwise `SHOW TABLES` / `system.tables` would list local table names according to the
    /// configured remote user rather than the caller's privileges.
    const Cluster * remote_cluster = cluster.get();
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (!shard_info.isLocal())
            continue;

        if (auto local_database = tryGetLocalDatabase())
        {
            /// The local database may be another `Remote` database that (indirectly) refers back
            /// to this one; the guard rejects such a cycle instead of recursing forever.
            LocalTraversalGuard guard(this);
            /// `getTablesIterator` returns every attached table regardless of the caller's grants, so
            /// filter by the caller's own `SHOW TABLES` right on the underlying local table, exactly
            /// like `system.tables` does. Otherwise a user with `SHOW TABLES` on the proxy database
            /// but no grants on the underlying local database could enumerate all of its table names
            /// through `Remote('127.0.0.1', ...)`.
            const auto access = local_context->getAccess();
            const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES, remote_database);
            Strings tables;
            for (auto it = local_database->getTablesIterator(query_context); it->isValid(); it->next())
            {
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, remote_database, it->name()))
                    continue;
                tables.push_back(it->name());
            }
            return tables;
        }

        /// The local replica does not have the database, but it may still exist on the remote
        /// replicas (e.g. `Remote('127.0.0.1|other_host', db)`), which the `Distributed` read path
        /// would fall back to (see `SelectStreamFactory::createForShard`); do the same here instead
        /// of hiding their tables. The fallback queries only the genuinely remote replicas: a TCP
        /// self-connection would list local metadata under the stored engine credentials rather
        /// than the caller's own.
        if (!remote_only_cluster)
            return {};
        remote_cluster = remote_only_cluster.get();
        break;
    }

    Strings tables;
    std::string fail_messages;
    for (const auto & shard_info : remote_cluster->getShardsInfo())
    {
        try
        {
            RemoteQueryExecutor executor(shard_info.pool, query, sample_block, query_context);
            executor.setPoolMode(PoolMode::GET_ONE);

            for (Block block = executor.readBlock(); !block.empty(); block = executor.readBlock())
            {
                const IColumn & name_column = *block.getByName("name").column;
                for (size_t i = 0, size = name_column.size(); i < size; ++i)
                    tables.push_back(name_column[i].safeGet<String>());
            }

            executor.finish();
            return tables;
        }
        catch (const NetException &)
        {
            fail_messages += getCurrentExceptionMessage(/* with_stacktrace = */ false) + '\n';
            continue;
        }
    }

    throw NetException(
        ErrorCodes::NO_REMOTE_SHARD_AVAILABLE, "All attempts to get the list of tables of the remote database failed. Log:\n\n{}\n", fail_messages);
}


ColumnsDescription DatabaseRemote::fetchTableStructure(const String & table_name, ContextPtr local_context, ClusterPtr & table_cluster) const
{
    table_cluster = cluster;

    /// A shard that points to this server is handled locally, like in `fetchTablesList`. Crucially, the
    /// local shard must not go through `DatabaseCatalog::getTable` (as the local-shard special case of
    /// `getStructureOfRemoteTable` does): for a missing table that method builds name hints, and the
    /// hints enumerate the tables of every database, including this one, recursing back into `fetchTable`
    /// and hanging the server. Resolve the table with the non-throwing methods instead.
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (!shard_info.isLocal())
            continue;

        if (auto local_database = tryGetLocalDatabase())
        {
            /// The local database may be another `Remote` database that (indirectly) refers back to
            /// this one; the guard rejects such a cycle instead of recursing forever.
            LocalTraversalGuard guard(this);
            if (auto storage = local_database->tryGetTable(table_name, local_context))
            {
                /// Resolution reads the structure of the underlying local table, so it requires the
                /// same right as `DESCRIBE TABLE` on it (like the local-shard special case of
                /// `getStructureOfRemoteTable`). The check applies only when the local replica
                /// actually serves the table: on the remote-replica fallback below no local object
                /// is touched, so a caller without any grants on the local objects must not be
                /// rejected there.
                local_context->checkAccess(AccessType::SHOW_COLUMNS, remote_database, table_name);
                auto metadata_snapshot = storage->getInMemoryMetadataPtr(local_context, /* bypass_metadata_cache = */ false);
                return metadata_snapshot->getColumns();
            }
        }

        /// The local replica does not have the database or the table, but it may still exist on the
        /// remote replicas (e.g. `Remote('127.0.0.1|other_host', db)`), so fall back to them instead
        /// of reporting the table as missing. The remote-only cluster keeps the fallback off a TCP
        /// self-connection, which would resolve local metadata under the stored engine credentials
        /// rather than the caller's own; it also becomes the cluster of the resulting `Distributed`
        /// storage, because reads and writes cannot use the local replica either (a missing local
        /// database fails `Context::resolveStorageID` before the remote-replica fallback of
        /// `SelectStreamFactory::createForShard` could engage, and an `INSERT` duplicates the data
        /// to every replica of the shard, so it must not fail on the local one).
        if (!remote_only_cluster)
            return {};

        table_cluster = remote_only_cluster;
        return getStructureOfRemoteTable(*remote_only_cluster, StorageID{remote_database, table_name}, local_context);
    }

    return getStructureOfRemoteTable(*cluster, StorageID{remote_database, table_name}, local_context);
}


StoragePtr DatabaseRemote::fetchTable(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    ColumnsDescription columns;
    ClusterPtr table_cluster;
    try
    {
        columns = fetchTableStructure(table_name, local_context, table_cluster);
    }
    catch (const Exception & e)
    {
        /// A genuinely missing table is a normal outcome (e.g. `EXISTS TABLE`): return `nullptr` on both
        /// the best-effort and the throwing path, so `SHOW CREATE TABLE` reports "does not exist" rather
        /// than the raw remote error.
        if (e.code() == ErrorCodes::UNKNOWN_TABLE)
            return {};

        /// A transport / authentication failure is not a missing table. On the throwing path (e.g.
        /// `SHOW CREATE TABLE`) propagate it instead of masquerading it as `UNKNOWN_TABLE`.
        if (throw_on_error)
            throw;

        /// The best-effort methods (`tryGetTable`, `isTableExist`, ...) can be called, for example, by a
        /// query to `system.tables`, which must not fail because of a remote error, so log only at debug
        /// level to avoid spurious errors in the server log and in the query's `stderr`.
        LOG_DEBUG(
            log,
            "Cannot get the structure of the remote table {}.{}: {}",
            remote_database,
            table_name,
            getCurrentExceptionMessage(/* with_stacktrace = */ false));
        return {};
    }
    catch (...)
    {
        if (throw_on_error)
            throw;

        LOG_DEBUG(
            log,
            "Cannot get the structure of the remote table {}.{}: {}",
            remote_database,
            table_name,
            getCurrentExceptionMessage(/* with_stacktrace = */ false));
        return {};
    }

    if (columns.empty())
        return {};

    /// Each table is a `Distributed` storage over the ad-hoc cluster built from the database's addresses.
    /// The empty `relative_data_path` makes every `INSERT` synchronous (see `StorageDistributed::write`),
    /// so no on-disk spool or background sender is needed and the storage requires no `startup`.
    /// `is_remote_database_proxy` additionally enforces the caller's own `SELECT`/`INSERT` rights on
    /// the underlying local table when a shard points to this server: by the time the storage is
    /// resolved, only `SHOW_COLUMNS` has been validated (see `fetchTableStructure`), which is not
    /// enough for a proxy that can read and write local data (the `remote` table function performs
    /// the same check in `TableFunctionRemote::executeImpl`); it also rejects `TRUNCATE`, which
    /// would otherwise be a silent no-op of `StorageDistributed`.
    return std::make_shared<StorageDistributed>(
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        /* comment = */ String{},
        remote_database,
        table_name,
        /* cluster_name_ = */ String{},
        getContext(),
        /* sharding_key_ = */ nullptr,
        /* storage_policy_name_ = */ "default",
        /* relative_data_path_ = */ String{},
        local_context->getDistributedSettings(),
        LoadingStrictnessLevel::ATTACH,
        table_cluster,
        /* remote_table_function_ptr_ = */ nullptr,
        /* is_remote_function_ = */ true,
        /* is_remote_database_proxy_ = */ true);
}


bool DatabaseRemote::empty() const
{
    return fetchTablesList(getContext()).empty();
}


DatabaseTablesIteratorPtr DatabaseRemote::getTablesIterator(
    ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */) const
{
    Tables tables;

    /// Do not allow to throw here, because this might be, for example, a query to `system.tables`.
    /// It must not fail because of a remote error.
    try
    {
        for (const auto & table_name : fetchTablesList(local_context))
        {
            if (filter_by_table_name && !filter_by_table_name(table_name))
                continue;

            if (auto storage = fetchTable(table_name, local_context))
                tables[table_name] = storage;
        }
    }
    catch (...)
    {
        /// Log only at debug level: an error-level log entry would be reported as an error of the
        /// query (e.g. of a `SHOW TABLES` covering this database) even though the query succeeds.
        LOG_DEBUG(log, "Cannot list the tables of the remote database: {}", getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}


std::vector<LightWeightTableDetails> DatabaseRemote::getLightweightTablesIterator(
    ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */) const
{
    /// `SHOW TABLES` only needs the names, so drive it straight from `fetchTablesList` instead of the
    /// structure-resolving `getTablesIterator`: the latter drops any table whose `DESC TABLE` fails,
    /// which would make `SHOW TABLES` hide a table that the remote `system.tables` query just returned.
    std::vector<LightWeightTableDetails> result;

    /// Do not allow to throw here for the same reason as in `getTablesIterator`.
    try
    {
        for (const auto & table_name : fetchTablesList(local_context))
        {
            if (filter_by_table_name && !filter_by_table_name(table_name))
                continue;

            result.emplace_back(LightWeightTableDetails{table_name});
        }
    }
    catch (...)
    {
        /// Log only at debug level for the same reason as in `getTablesIterator`.
        LOG_DEBUG(log, "Cannot list the tables of the remote database: {}", getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }

    return result;
}


VectorWithMemoryTracking<String> DatabaseRemote::getAllTableNames(ContextPtr local_context) const
{
    /// Only the names are requested (e.g. by the name hints for a missing table), so skip inferring
    /// the structure of every table, which the default implementation would do through
    /// `getTablesIterator` at the cost of a `DESC TABLE` round trip per table.
    VectorWithMemoryTracking<String> result;

    /// Do not allow to throw here for the same reason as in `getTablesIterator`.
    try
    {
        for (auto & table_name : fetchTablesList(local_context ? local_context : getContext()))
            result.emplace_back(std::move(table_name));
    }
    catch (...)
    {
        /// Log only at debug level for the same reason as in `getTablesIterator`.
        LOG_DEBUG(log, "Cannot list the tables of the remote database: {}", getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }

    return result;
}


bool DatabaseRemote::isTableExist(const String & table_name, ContextPtr local_context) const
{
    /// `isTableExist` is user-visible: `EXISTS TABLE` reaches it through `InterpreterExistsQuery`.
    /// Resolve with `throw_on_error = true` (like `tryGetTable`) so that only a genuinely missing
    /// table yields `false`, while a transport/authentication failure on an existing remote table is
    /// propagated as the real error instead of being reported as "does not exist".
    return static_cast<bool>(fetchTable(table_name, local_context, /* throw_on_error = */ true));
}


StoragePtr DatabaseRemote::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    /// This is the ordinary table-resolution path of user queries (`SELECT`, `INSERT`, ...), both
    /// through `IDatabase::getTable` and through the analyzer's `DatabaseCatalog::tryGetTable`.
    /// Return `nullptr` only for a genuinely missing table; a transport/authentication failure while
    /// resolving an existing remote table is propagated as the real remote error rather than being
    /// reported as `UNKNOWN_TABLE` (`DatabaseMySQL::tryGetTable` behaves the same way on a connection
    /// failure). The listing paths (`getTablesIterator`, `getAllTableNames`, `isTableExist`) stay
    /// best-effort.
    return fetchTable(table_name, local_context, /* throw_on_error = */ true);
}


ASTPtr DatabaseRemote::getCreateDatabaseQueryImpl() const
{
    const auto & create_query = make_intrusive<ASTCreateQuery>();
    create_query->setDatabase(database_name);
    create_query->set(create_query->storage, database_engine_define->clone());
    create_query->uuid = db_uuid;

    if (!comment.empty())
        create_query->set(create_query->comment, make_intrusive<ASTLiteral>(comment));

    return create_query;
}


ASTPtr DatabaseRemote::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    auto storage = fetchTable(table_name, local_context, throw_on_error);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist on the remote server", remote_database, table_name);
        return nullptr;
    }

    /// The table is exposed as the `Remote`/`RemoteSecure` table engine, which is the persistent
    /// counterpart of the `remote`/`remoteSecure` table functions. Turn the database engine
    /// definition (`Remote('addresses', 'remote_db'[, 'user'[, 'password']])`) into a table engine by
    /// inserting the remote table name right after the remote database name.
    auto table_storage_define = database_engine_define->clone();
    {
        ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
        ast_storage->engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
        ast_storage->reset(ast_storage->settings);

        auto & engine_arguments = ast_storage->engine->arguments->children;
        if (typeid_cast<ASTIdentifier *>(engine_arguments[0].get()))
        {
            /// The addresses are given as a named collection: append `table = '<name>'`.
            engine_arguments.push_back(
                makeASTOperator("equals", make_intrusive<ASTIdentifier>("table"), make_intrusive<ASTLiteral>(table_name)));
        }
        else
        {
            /// Positional arguments: `addresses, remote_db[, user[, password]]` -> insert the table name at index 2.
            engine_arguments.insert(engine_arguments.begin() + 2, make_intrusive<ASTLiteral>(table_name));
        }
    }

    /// Reuse the common serializer with `only_ordinary = false` so that column defaults, aliases and
    /// materialized expressions inferred from the remote `ClickHouse` table are preserved. Emitting only
    /// names and types would change insert/read semantics of the recreated `Remote(...)` table.
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


void DatabaseRemote::createTable(ContextPtr, const String & table_name, const StoragePtr &, const ASTPtr &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The `{}` database engine is a read-through view of a remote server and does not support CREATE TABLE (table {})",
        getEngineName(),
        table_name);
}


void DatabaseRemote::dropTable(ContextPtr, const String & table_name, bool /* sync */)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The `{}` database engine is a read-through view of a remote server and does not support DROP TABLE (table {})",
        getEngineName(),
        table_name);
}


void DatabaseRemote::attachTable(ContextPtr, const String & table_name, const StoragePtr &, const String &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The `{}` database engine is a read-through view of a remote server and does not support ATTACH TABLE (table {})",
        getEngineName(),
        table_name);
}


StoragePtr DatabaseRemote::detachTable(ContextPtr, const String & table_name)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The `{}` database engine is a read-through view of a remote server and does not support DETACH TABLE (table {})",
        getEngineName(),
        table_name);
}


void DatabaseRemote::drop(ContextPtr)
{
    if (!persistent)
        return;

    auto db_disk = getDisk();
    db_disk->removeRecursive(getMetadataPath());
}


struct DatabaseRemoteClusters
{
    ClusterPtr cluster;
    /// See `DatabaseRemote::remote_only_cluster`.
    ClusterPtr remote_only_cluster;
};

/// Builds an ad-hoc cluster from the addresses expression, exactly like the `remote`/`remoteSecure`
/// table functions do. In particular, an address that points to this server is treated as a local
/// shard (`treat_local_as_remote = false`), so `SELECT`/`INSERT` run directly instead of opening a
/// self-connection; this matches the well-tested behavior of the `remote` table function and the
/// `Remote` storage engine and, as for them, means the stored credentials are honored only for
/// genuinely remote shards.
static DatabaseRemoteClusters buildClusters(const String & cluster_description, const String & username, const String & password, bool secure, ContextPtr context)
{
    size_t max_addresses = context->getSettingsRef()[Setting::table_function_remote_max_addresses];
    Strings shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

    HostsByShard names;
    names.reserve(shards.size());
    for (const auto & shard : shards)
    {
        auto replicas = parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses);
        if (replicas.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard contains zero number of replicas");
        names.push_back(std::move(replicas));
    }

    if (names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard list is empty after parsing the addresses");

    auto maybe_secure_port = context->getTCPPortSecure();
    const UInt16 default_port
        = secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort();

    /// Check host and port against the allowed hosts filter.
    for (const auto & hosts : names)
    {
        for (const auto & host : hosts)
        {
            size_t colon = host.find(':');
            if (colon == String::npos)
                context->getRemoteHostFilter().checkHostAndPort(host, toString(default_port));
            else
                context->getRemoteHostFilter().checkHostAndPort(host.substr(0, colon), host.substr(colon + 1));
        }
    }

    ClusterConnectionParameters params{
        username,
        password,
        default_port,
        /* treat_local_as_remote = */ false,
        /* treat_local_port_as_remote = */ context->getApplicationType() == Context::ApplicationType::LOCAL,
        secure,
        /* bind_host = */ "",
        /* priority = */ Priority{1},
        /* cluster_name = */ "",
        /* cluster_secret = */ ""};

    auto all_replicas_cluster = std::make_shared<Cluster>(context->getSettingsRef(), names, params);

    /// A replica that points to this server is resolved through the local catalog. When it does not
    /// have the database or the table, the metadata lookup falls back to the remaining replicas of
    /// the same shard (see `fetchTablesList` / `fetchTableStructure`), which must reach only the
    /// genuinely remote ones, so precompute a cluster with the local replicas stripped from their
    /// shards while every other shard stays intact. If some shard consists of local replicas only,
    /// there is nothing to fall back to for that shard, and a fallback cluster without it would
    /// silently read and write only a subset of the configured shards, so no fallback cluster is
    /// built at all: a database or table missing on such a local replica is reported as missing.
    HostsByShard remote_only_names;
    bool has_local_replicas = false;
    bool fallback_possible = true;
    for (const auto & shard_addresses : all_replicas_cluster->getShardsAddresses())
    {
        Strings replicas;
        bool shard_has_local_replicas = false;
        for (const auto & address : shard_addresses)
        {
            if (address.is_local)
                shard_has_local_replicas = true;
            else
                replicas.push_back(address.readableString());
        }
        has_local_replicas |= shard_has_local_replicas;
        if (shard_has_local_replicas && replicas.empty())
        {
            fallback_possible = false;
            break;
        }
        remote_only_names.push_back(std::move(replicas));
    }

    ClusterPtr remote_only_cluster;
    if (has_local_replicas && fallback_possible)
        remote_only_cluster = std::make_shared<Cluster>(context->getSettingsRef(), remote_only_names, params);

    return {std::move(all_replicas_cluster), std::move(remote_only_cluster)};
}


void registerDatabaseRemote(DatabaseFactory & factory);
void registerDatabaseRemote(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args) -> DatabasePtr
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine->name;
        const bool secure = engine_name == "RemoteSecure";

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        ASTs & engine_args = engine->arguments->children;

        String addresses_expr;
        String remote_database;
        String username = "default";
        String password;

        if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, args.context))
        {
            validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(
                *named_collection,
                {"addresses_expr", "host", "hostname", "database", "db"},
                {"username", "user", "password", "port"});

            addresses_expr = named_collection->getOrDefault<String>("addresses_expr", "");
            if (addresses_expr.empty())
            {
                const String host = named_collection->getAny<String>({"host", "hostname"});
                addresses_expr = named_collection->has("port") ? host + ':' + toString(named_collection->get<UInt64>("port")) : host;
            }
            remote_database = named_collection->getAnyOrDefault<String>({"database", "db"}, "default");
            username = named_collection->getAnyOrDefault<String>({"username", "user"}, "default");
            password = named_collection->getOrDefault<String>("password", "");
        }
        else
        {
            if (engine_args.size() < 2 || engine_args.size() > 4)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Engine `{}` requires from 2 to 4 arguments: 'addresses_expr', 'database'[, 'user'[, 'password']]",
                    engine_name);

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

            addresses_expr = safeGetLiteralValue<String>(engine_args[0], engine_name);
            remote_database = safeGetLiteralValue<String>(engine_args[1], engine_name);
            if (engine_args.size() >= 3)
                username = safeGetLiteralValue<String>(engine_args[2], engine_name);
            if (engine_args.size() >= 4)
                password = safeGetLiteralValue<String>(engine_args[3], engine_name);
        }

        auto clusters = buildClusters(addresses_expr, username, password, secure, args.context);

        return std::make_shared<DatabaseRemote>(
            args.context,
            args.metadata_path,
            engine_define,
            args.database_name,
            remote_database,
            std::move(clusters.cluster),
            std::move(clusters.remote_only_cluster),
            secure,
            args.uuid);
    };

    const auto features = DatabaseFactory::EngineFeatures{
        .supports_arguments = true,
        .supports_settings = false,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::REMOTE,
    };

    const String common_description = R"DOCS_MD(
The `Remote` and `RemoteSecure` database engines provide real-time access to the tables of a database on a remote ClickHouse server over the native TCP protocol. They are the ClickHouse-to-ClickHouse counterparts of the [`MySQL`](/engines/database-engines/mysql) and [`PostgreSQL`](/engines/database-engines/postgresql) database engines.

The list of tables and their structure are fetched from the remote server on demand, so the database always reflects its current state. Each table is exposed as a [`Distributed`](/engines/table-engines/special/distributed) storage over an ad-hoc cluster built from the supplied addresses, which forwards `SELECT` and `INSERT` queries to the remote server. This is handy for federating several ClickHouse clusters or for plugging a larger cluster into `clickhouse-local` or a smaller cluster.
)DOCS_MD";

    factory.registerDatabase(
        "Remote",
        create_fn,
        features,
        Documentation{
            .description = common_description + R"DOCS_MD(
`Remote` connects over the plain TCP port (`tcp_port`, `9000` by default) when the port is omitted.
)DOCS_MD",
            .syntax = "ENGINE = Remote('addresses_expr', 'database'[, 'user'[, 'password']])",
            .related = {"MySQL", "PostgreSQL"}});

    factory.registerDatabase(
        "RemoteSecure",
        create_fn,
        features,
        Documentation{
            .description = common_description + R"DOCS_MD(
`RemoteSecure` connects over a secure TLS connection using the secure TCP port (`tcp_port_secure`, `9440` by default) when the port is omitted.
)DOCS_MD",
            .syntax = "ENGINE = RemoteSecure('addresses_expr', 'database'[, 'user'[, 'password']])",
            .related = {"MySQL", "PostgreSQL"}});
}

}
