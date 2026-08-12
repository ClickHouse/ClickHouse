#include <Databases/DatabaseRemote.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/Names.h>
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
#include <Common/parseAddress.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

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
    extern const int NO_REMOTE_SHARD_AVAILABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}


DatabaseRemote::DatabaseRemote(
    ContextPtr context_,
    const String & metadata_path_,
    const ASTStorage * database_engine_define_,
    const String & database_name_,
    const String & remote_database_,
    const String & username_,
    const String & password_,
    ClusterPtr cluster_,
    ClusterPtr remote_only_cluster_,
    bool secure_,
    UUID uuid)
    : DatabaseWithAltersOnDiskBase(database_name_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , remote_database(remote_database_)
    , username(username_)
    , password(password_)
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


Strings DatabaseRemote::fetchTablesList(ContextPtr local_context, const String * only_table, bool ignore_visibility) const
{
    auto sample_block = std::make_shared<const Block>(Block{
        {ColumnString::create(), std::make_shared<DataTypeString>(), "name"},
    });

    String query = fmt::format("SELECT name FROM system.tables WHERE database = {}", quoteString(remote_database));

    /// An existence check (`EXISTS TABLE`) needs a single name, so do not pull the whole list.
    if (only_table)
        query += fmt::format(" AND name = {}", quoteString(*only_table));

    /// This is a service query, not a real user query, so it must not fail because of the user's
    /// result-size limits (e.g. `max_result_rows` set for the current query).
    auto query_context = Context::createCopy(local_context);
    {
        Settings new_settings = query_context->getSettingsCopy();
        new_settings[Setting::max_result_rows] = 0;
        new_settings[Setting::max_result_bytes] = 0;
        query_context->setSettings(new_settings);
    }

    /// Ask the replicas of the cluster for the list of names, taking the answer of the first one that
    /// responds (`PoolMode::GET_ONE`), and report the failed attempts when none of them does.
    auto fetch_from_cluster = [&](const Cluster & remote_cluster)
    {
        std::string fail_messages;
        for (const auto & shard_info : remote_cluster.getShardsInfo())
        {
            try
            {
                RemoteQueryExecutor executor(shard_info.pool, query, sample_block, query_context);
                executor.setPoolMode(PoolMode::GET_ONE);

                /// Accumulate per attempt: names read before a mid-stream failure must not leak into
                /// the next attempt, or a retried listing would return a mixed or duplicated result.
                Strings tables;
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
            ErrorCodes::NO_REMOTE_SHARD_AVAILABLE,
            "All attempts to get the list of tables of the remote database failed. Log:\n\n{}\n",
            fail_messages);
    };

    /// The metadata (the list of the tables and, in `fetchTableStructure`, their structure) is resolved
    /// from an arbitrary shard, exactly like `getStructureOfRemoteTable` does: the shards of one cluster
    /// normally serve the same set of tables, and asking every one of them would multiply the cost of
    /// every `SHOW TABLES` / `EXISTS TABLE` by the number of shards. A shard that points to this server
    /// is preferred, because it needs no round trip at all; another shard is consulted only when the
    /// current one is unavailable.
    const Cluster * remote_cluster = cluster.get();
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        /// A shard that points to this server is a local shard (see `buildClusters`). Enumerate the
        /// local database under `local_context` instead of opening a self-connection with the stored
        /// engine credentials, mirroring the local-shard special case of `getStructureOfRemoteTable`.
        /// Otherwise `SHOW TABLES` / `system.tables` would list local table names according to the
        /// configured remote user rather than the caller's privileges.
        if (!shard_info.isLocal())
            continue;

        if (auto local_database = tryGetLocalDatabase())
        {
            /// The local database may be another `Remote` database that (indirectly) refers back
            /// to this one; the guard rejects such a cycle instead of recursing forever.
            LocalTraversalGuard guard(this);
            /// The underlying tables are enumerated regardless of the caller's grants, so filter by
            /// the caller's own `SHOW TABLES` right on the underlying local table, exactly like
            /// `system.tables` does. Otherwise a user with `SHOW TABLES` on the proxy database but
            /// no grants on the underlying local database could enumerate all of its table names
            /// through `Remote('127.0.0.1', ...)`.
            /// ... unless the underlying database is itself a `Remote` database: its own listing is
            /// already filtered by the caller's rights, but on the objects that it proxies in turn,
            /// which live under a different database name. Checking the name of the intermediate
            /// proxy on top of that would hide the tables the caller is in fact allowed to see (a
            /// chain `outer` -> `inner` -> `db` needs no grants on `inner`, only on `db`).
            const auto * underlying_remote = typeid_cast<const DatabaseRemote *>(local_database.get());
            const bool underlying_listing_is_filtered_by_access = underlying_remote != nullptr;
            const auto access = local_context->getAccess();

            try
            {
                if (only_table)
                {
                    /// Existence is a property of the name, so it must not depend on whether the table
                    /// can be described. A table invisible to the caller is reported as missing, like in
                    /// the listing below. When the local replica does not have the table, fall through to
                    /// the remote-replica fallback, mirroring `fetchTableStructure`.
                    ///
                    /// The name is resolved ignoring the caller's visibility first, so that a table hidden
                    /// from the caller can be told from a genuinely missing one: only the latter may fall
                    /// through to the same-shard remote replicas below, which would otherwise resolve —
                    /// under the stored engine credentials — exactly the table that this database hides.
                    /// `IDatabase::isTableExist` is already visibility-agnostic; a nested `Remote` database
                    /// filters its own answer, so it is asked explicitly.
                    const bool name_exists = underlying_remote
                        ? underlying_remote->isTableExistIgnoringVisibility(*only_table, query_context)
                        : local_database->isTableExist(*only_table, query_context);

                    if (name_exists)
                    {
                        if (ignore_visibility)
                            return Strings{*only_table};

                        if (underlying_remote)
                            return underlying_remote->isTableExist(*only_table, query_context) ? Strings{*only_table} : Strings{};

                        if (access->isGranted(AccessType::SHOW_TABLES, remote_database, *only_table))
                            return Strings{*only_table};
                        return {};
                    }
                }
                else
                {
                    /// Enumerate through the lightweight iterator: only the names are needed here, and the
                    /// underlying database may itself be an external one (e.g. another `Remote` database),
                    /// whose plain `getTablesIterator` resolves the structure of every table and drops
                    /// those that fail.
                    const bool check_access_for_tables = !underlying_listing_is_filtered_by_access
                        && !access->isGranted(AccessType::SHOW_TABLES, remote_database);
                    Strings tables;
                    NameSet local_names;
                    for (const auto & table : local_database->getLightweightTablesIterator(query_context))
                    {
                        local_names.insert(table.name);
                        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, remote_database, table.name))
                            continue;
                        tables.push_back(table.name);
                    }

                    /// The local replica has the database, but it may still be missing a table that another
                    /// replica of the shard has, and resolution (`EXISTS TABLE`, `SELECT`) does fall back to
                    /// that replica, so the listing has to include such a table as well; otherwise the same
                    /// name would be absent from `SHOW TABLES` and `system.tables` while it can be described
                    /// and read. Only a name that the local replica does not have at all is taken from the
                    /// fallback: a name it has but hides from the caller stays hidden, exactly like in the
                    /// `only_table` branch above.
                    if (!remote_only_cluster)
                        return tables;

                    Strings remote_tables;
                    try
                    {
                        remote_tables = fetch_from_cluster(*remote_only_cluster);
                    }
                    catch (...)
                    {
                        /// The metadata is resolved from an arbitrary available replica, and the local one
                        /// has just answered, so its list is a valid answer on its own: an unreachable
                        /// second replica must not turn a `SHOW TABLES` that the local replica can serve
                        /// into an error. Log at debug level for the same reason as in `getTablesIterator`.
                        LOG_DEBUG(
                            log,
                            "Cannot complete the list of the tables of the remote database {} from its remote replicas: {}",
                            backQuoteIfNeed(remote_database),
                            getCurrentExceptionMessage(/* with_stacktrace = */ false));
                        return tables;
                    }

                    for (const auto & table_name : remote_tables)
                    {
                        if (local_names.contains(table_name))
                            continue;

                        /// A table that the lightweight iterator did not return can still exist locally and
                        /// merely be invisible to the caller (a nested `Remote` database filters its own
                        /// listing), and such a name must not be served by another replica.
                        const bool exists_locally = underlying_remote
                            ? underlying_remote->isTableExistIgnoringVisibility(table_name, query_context)
                            : local_database->isTableExist(table_name, query_context);
                        if (exists_locally)
                            continue;

                        tables.push_back(table_name);
                    }

                    return tables;
                }
            }
            catch (const NetException &)
            {
                /// The local database is itself another `Remote` database, and the failure came from its
                /// own remote target, not from this one: an answer of the local replica of this shard is
                /// simply not available, exactly as if the replica itself were down. Fall through to the
                /// same-shard remote replicas below instead of turning one bad intermediate proxy into a
                /// failure of the whole database. The hidden-vs-missing distinction above is a property
                /// of the answering replica, and the answering replica is now a remote one.
                if (!underlying_remote || !remote_only_cluster)
                    throw;
                LOG_DEBUG(
                    log,
                    "Cannot list the tables of the remote database {} through the local database: {}. Trying the remote replicas",
                    backQuoteIfNeed(remote_database),
                    getCurrentExceptionMessage(/* with_stacktrace = */ false));
            }
        }

        /// The local replica does not have the database (or the requested table), but it may still
        /// exist on the remote replicas (e.g. `Remote('127.0.0.1|other_host', db)`), which the
        /// `Distributed` read path would fall back to (see `SelectStreamFactory::createForShard`); do
        /// the same here instead of hiding their tables. The fallback queries only the genuinely
        /// remote replicas: a TCP self-connection would list local metadata under the stored engine
        /// credentials rather than the caller's own.
        if (!remote_only_cluster)
            return {};
        remote_cluster = remote_only_cluster.get();
        break;
    }

    return fetch_from_cluster(*remote_cluster);
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
            /// The underlying database may itself be a `Remote` database: `tryGetTable` below has then
            /// already validated the caller's `SHOW_COLUMNS` right on the objects that it proxies in
            /// turn, which live under a different database name, so checking the name of the
            /// intermediate proxy on top of that would only demand a grant on an object that holds no
            /// metadata of its own, and would hide from the caller a table it may in fact describe (a
            /// chain `outer` -> `inner` -> `db` needs no grants on `inner`, only on `db`) — exactly
            /// like in `fetchTablesList`. Reading and writing the data still requires the rights on
            /// every hop of the chain, because the query is really executed against the table of the
            /// intermediate database, as it is for a `Distributed` table over another one.
            const auto * underlying_remote = typeid_cast<const DatabaseRemote *>(local_database.get());
            const bool local_database_is_remote = underlying_remote != nullptr;

            /// `IDatabase::tryGetTable` resolves the name regardless of the caller's grants, so the
            /// mere fact that the local table exists must not become observable here: without this
            /// filter a caller holding rights only on the proxy database could probe arbitrary names
            /// of the underlying local database, because an existing table would be rejected by the
            /// `SHOW_COLUMNS` check below while a missing one falls through to "does not exist".
            /// A name the caller cannot see at all is therefore reported as missing, exactly like in
            /// the listing of `fetchTablesList`. A name that is already visible to the caller keeps
            /// the explicit `ACCESS_DENIED` of the `SHOW_COLUMNS` check, which reveals nothing new
            /// (any grant on the table, `SHOW_COLUMNS` included, implies its `SHOW TABLES` visibility).
            const bool name_is_visible = local_database_is_remote
                || local_context->getAccess()->isGranted(AccessType::SHOW_TABLES, remote_database, table_name);

            try
            {
                if (name_is_visible)
                {
                    if (auto storage = local_database->tryGetTable(table_name, local_context))
                    {
                        /// Resolution reads the structure of the underlying local table, so it requires the
                        /// same right as `DESCRIBE TABLE` on it (like the local-shard special case of
                        /// `getStructureOfRemoteTable`). The check applies only when the local replica
                        /// actually serves the table: on the remote-replica fallback below no local object
                        /// is touched, so a caller without any grants on the local objects must not be
                        /// rejected there.
                        if (!local_database_is_remote)
                            local_context->checkAccess(AccessType::SHOW_COLUMNS, remote_database, table_name);
                        auto metadata_snapshot = storage->getInMemoryMetadataPtr(local_context, /* bypass_metadata_cache = */ false);
                        auto columns = metadata_snapshot->getColumns();

                        /// The columns can come back empty because of a race with concurrent DDL (e.g.
                        /// `REPLACE TABLE` or lazy storage initialization). The table exists, so the empty
                        /// set must not be returned as a successful resolution: `fetchTable` interprets an
                        /// empty structure as an absent table, misreporting the race as `UNKNOWN_TABLE`.
                        /// Treat it as a transient condition instead, exactly like `getStructureOfRemoteTable`
                        /// does: fall through to the same-shard remote replicas, and when there are none,
                        /// report the failed attempt so the caller can retry.
                        if (!columns.empty())
                            return columns;

                        if (!remote_only_cluster)
                            throw NetException(
                                ErrorCodes::NO_REMOTE_SHARD_AVAILABLE,
                                "The table {}.{} exists on the local shard, but its structure is temporarily unavailable "
                                "(e.g. because of a concurrent REPLACE TABLE or lazy storage initialization), "
                                "and there are no remote replicas to fall back to. Retry the query",
                                backQuoteIfNeed(remote_database),
                                backQuoteIfNeed(table_name));
                    }
                    else if (underlying_remote && underlying_remote->isTableExistIgnoringVisibility(table_name, local_context))
                    {
                        /// A nested `Remote` database answers `tryGetTable` with `nullptr` both for a table
                        /// that it hides from the caller and for a genuinely missing one, and only the
                        /// latter may use the fallback below: otherwise the outer proxy would resolve —
                        /// through another replica of the same shard, under the stored engine credentials —
                        /// the very table that the intermediate database hides, while `SHOW TABLES` and
                        /// `EXISTS TABLE` keep reporting it as missing (see the direct local table below).
                        return {};
                    }
                }
                else if (local_database->isTableExist(table_name, local_context))
                {
                    /// The name does exist on the local shard, but the caller cannot see it. Reporting it
                    /// as missing is not enough: falling through to the same-shard remote replicas below
                    /// would resolve the very table that `SHOW TABLES` and `EXISTS TABLE` hide (the
                    /// `only_table` branch of `fetchTablesList` stops at the local shard in exactly the
                    /// same situation), so a caller holding rights only on the proxy database could
                    /// `DESCRIBE`, `SHOW CREATE` or even `SELECT` a hidden local table through another
                    /// replica of its shard. The fallback exists for a table the local replica genuinely
                    /// lacks, not for a table it is not allowed to expose.
                    return {};
                }
            }
            catch (const NetException &)
            {
                /// The local database is itself another `Remote` database, and the failure came from its
                /// own remote target, not from this one: an answer of the local replica of this shard is
                /// simply not available, exactly as if the replica itself were down. Fall through to the
                /// same-shard remote replicas below instead of turning one bad intermediate proxy into a
                /// failure of the whole database (`fetchTablesList` does the same for the listing). The
                /// hidden-vs-missing distinction above is a property of the answering replica, and the
                /// answering replica is now a remote one.
                if (!local_database_is_remote || !remote_only_cluster)
                    throw;
                LOG_DEBUG(
                    log,
                    "Cannot resolve the table {}.{} through the local database: {}. Trying the remote replicas",
                    backQuoteIfNeed(remote_database),
                    backQuoteIfNeed(table_name),
                    getCurrentExceptionMessage(/* with_stacktrace = */ false));
            }
        }

        /// The local replica does not have the database or the table (or the structure of its local
        /// table is temporarily empty, see above), but the table may still exist on the remote
        /// replicas (e.g. `Remote('127.0.0.1|other_host', db)`), so fall back to them instead
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
    ///
    /// A proxy over more than one shard gets an implicit `rand()` sharding key, so that it is
    /// writable by default: without a sharding key, `StorageDistributed::write` rejects an `INSERT`
    /// into a multi-shard cluster (`STORAGE_REQUIRES_PARAMETER`) unless the caller sets
    /// `insert_shard_id` or `insert_distributed_one_random_shard` for the query, while the engine
    /// advertises that it forwards `INSERT` queries. Each row goes to a random shard, consistent
    /// with resolving the metadata from an arbitrary shard; an explicit `insert_shard_id` still
    /// pins the shard for a query. The key only distributes the inserted rows: the read path of
    /// `StorageDistributed` ignores it (see `hasShardingKeyForReads`), so shard pruning under
    /// `force_optimize_skip_unused_shards` treats the proxy as a table without a sharding key,
    /// exactly as before the key was introduced.
    ASTPtr sharding_key;
    if (table_cluster->getShardsInfo().size() > 1)
        sharding_key = makeASTFunction("rand");

    return std::make_shared<StorageDistributed>(
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        /* comment = */ String{},
        remote_database,
        table_name,
        /* cluster_name_ = */ String{},
        getContext(),
        sharding_key,
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
    /// The consumers of the plain iterator dereference the storage object unconditionally, so a table
    /// whose structure could not be fetched is dropped here.
    ///
    /// This iterator is not a user-facing listing: it is what the server itself walks over every
    /// database (asynchronous metrics, `SYSTEM` commands, `DROP DATABASE`, ...), so a single
    /// unreachable remote database must not make those operations fail; it stays best-effort. The
    /// user-facing listings (`SHOW TABLES`, `system.tables`) propagate the error instead.
    return getTablesIteratorImpl(
        local_context, filter_by_table_name, /* keep_unresolved_tables = */ false, /* throw_on_error = */ false);
}


DatabaseTablesIteratorPtr DatabaseRemote::getTablesIteratorWithHint(
    ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */, const TablesFilter & /*tables_filter*/) const
{
    /// This is the `system.tables` path, which null-guards every metadata column (see
    /// `StorageSystemTables`), so keep a table whose structure could not be fetched instead of hiding
    /// it: the name has already been established by `fetchTablesList`, and a row with an empty engine
    /// is a far better answer than a table that silently disappears from `system.tables` because the
    /// caller lacks `SHOW COLUMNS` on it or a single `DESC TABLE` failed.
    return getTablesIteratorImpl(
        local_context, filter_by_table_name, /* keep_unresolved_tables = */ true, /* throw_on_error = */ true);
}


DatabaseTablesIteratorPtr DatabaseRemote::getTablesIteratorImpl(
    ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool keep_unresolved_tables, bool throw_on_error) const
{
    Tables tables;

    try
    {
        for (const auto & table_name : fetchTablesList(local_context))
        {
            if (filter_by_table_name && !filter_by_table_name(table_name))
                continue;

            auto storage = fetchTable(table_name, local_context);
            if (storage || keep_unresolved_tables)
                tables[table_name] = storage;
        }
    }
    catch (...)
    {
        if (throw_on_error)
            throw;

        /// Log only at debug level: an error-level log entry would be reported as an error of the
        /// query even though the query succeeds.
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
    ///
    /// A failure of the remote listing is propagated: this is the explicit `SHOW TABLES` path (and the
    /// names-only path of `system.tables`), so an unreachable server has to be reported as such. An
    /// empty successful answer would contradict `EXISTS TABLE` and `SELECT` on the same database, which
    /// do report the real network or authentication error, and it would hide the outage behind "there
    /// are no tables". The helper paths (name hints, the plain `getTablesIterator` that the server
    /// itself walks) stay best-effort.
    std::vector<LightWeightTableDetails> result;

    for (const auto & table_name : fetchTablesList(local_context))
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        result.emplace_back(LightWeightTableDetails{table_name});
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
    /// Existence is a property of the name, so answer it from the table names rather than by
    /// resolving the structure through `fetchTable`: a table that exists but cannot be described
    /// (e.g. the caller lacks `SHOW_COLUMNS` on the underlying local table, or its `DESC TABLE`
    /// fails transiently) must still be reported as existing. Only a genuinely missing (or
    /// invisible) table yields `false`; a transport/authentication failure is propagated as the
    /// real error (like `tryGetTable`) instead of being reported as "does not exist".
    return !fetchTablesList(local_context, &table_name).empty();
}


bool DatabaseRemote::isTableExistIgnoringVisibility(const String & table_name, ContextPtr local_context) const
{
    return !fetchTablesList(local_context, &table_name, /* ignore_visibility = */ true).empty();
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

    /// The live proxy is bound to `remote_only_cluster` when the local replica does not have the
    /// database or the table (see `fetchTableStructure`). The configured addresses would then
    /// describe a different object: the `Remote` table engine has no such fallback, so a table
    /// recreated from them fails on the missing local objects (`SELECT`) or writes to the missing
    /// local replica (`INSERT`). Serialize the effective fallback addresses instead, so the emitted
    /// definition reconstructs the object that actually serves the queries.
    const auto * distributed = typeid_cast<const StorageDistributed *>(storage.get());

    String effective_addresses;
    if (remote_only_cluster)
    {
        if (distributed && distributed->getCluster() == remote_only_cluster)
        {
            for (const auto & shard_addresses : remote_only_cluster->getShardsAddresses())
            {
                if (!effective_addresses.empty())
                    effective_addresses += ',';
                for (size_t i = 0; i < shard_addresses.size(); ++i)
                {
                    if (i)
                        effective_addresses += '|';
                    effective_addresses += shard_addresses[i].readableString();
                }
            }
        }
    }

    /// A proxy over more than one shard carries an implicit `rand()` sharding key (see `fetchTable`).
    /// The `Remote` table engine accepts a trailing sharding key but does not add one on its own, so
    /// the key must be serialized explicitly: otherwise the emitted definition recreates a table that
    /// rejects a multi-shard `INSERT` (`STORAGE_REQUIRES_PARAMETER`), while the live proxy accepts it.
    const bool has_implicit_sharding_key = distributed && distributed->getCluster()->getShardsInfo().size() > 1;

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
            if (!effective_addresses.empty())
            {
                /// The addresses come from a named collection, whose address set cannot be replaced
                /// in the emitted definition: an `addresses_expr = ...` override next to a collection
                /// keyed by `host` fails the key validation of the engine when the definition is
                /// replayed. Emit the positional form instead, carrying the stored credentials
                /// explicitly; the password is masked by the usual secret hiding of
                /// `SHOW CREATE TABLE`, exactly as in a positional definition.
                engine_arguments = {
                    make_intrusive<ASTLiteral>(effective_addresses),
                    make_intrusive<ASTLiteral>(remote_database),
                    make_intrusive<ASTLiteral>(table_name),
                    make_intrusive<ASTLiteral>(username),
                    make_intrusive<ASTLiteral>(password)};
                if (has_implicit_sharding_key)
                    engine_arguments.push_back(makeASTFunction("rand"));
            }
            else
            {
                /// The addresses are given as a named collection: append `table = '<name>'`.
                engine_arguments.push_back(
                    makeASTOperator("equals", make_intrusive<ASTIdentifier>("table"), make_intrusive<ASTLiteral>(table_name)));
                if (has_implicit_sharding_key)
                    engine_arguments.push_back(
                        makeASTOperator("equals", make_intrusive<ASTIdentifier>("sharding_key"), makeASTFunction("rand")));
            }
        }
        else
        {
            /// Positional arguments: `addresses, remote_db[, user[, password]]` -> insert the table name at index 2.
            if (!effective_addresses.empty())
                engine_arguments[0] = make_intrusive<ASTLiteral>(effective_addresses);
            engine_arguments.insert(engine_arguments.begin() + 2, make_intrusive<ASTLiteral>(table_name));
            if (has_implicit_sharding_key)
                engine_arguments.push_back(makeASTFunction("rand"));
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

    /// Check host and port against the allowed hosts filter. The address is split with `parseAddress`,
    /// the same helper that `Cluster` uses, because a naive split at the first `:` would tear a
    /// bracketed IPv6 literal such as `[2001:db8::1]:9440` apart and check the nonsensical host `[`.
    /// The brackets are stripped, so the filter sees the bare address, as it does for a URL.
    for (const auto & hosts : names)
    {
        for (const auto & host : hosts)
        {
            auto [host_name, port] = parseAddress(host, default_port);
            if (host_name.size() >= 2 && host_name.front() == '[' && host_name.back() == ']')
                host_name = host_name.substr(1, host_name.size() - 2);
            context->getRemoteHostFilter().checkHostAndPort(host_name, toString(port));
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
                String host = named_collection->getAny<String>({"host", "hostname"});
                /// An IPv6 literal has to be bracketed before a port can be appended: the addresses are
                /// later split with `parseAddress`, which treats the first `:` of an unbracketed address
                /// as the port separator, so a bare `2001:db8::1` would be torn apart.
                if (host.contains(':') && !host.starts_with('['))
                    host = '[' + host + ']';
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
            username,
            password,
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

The list of tables and their structure are fetched from the remote server on demand (using `SHOW TABLES` and `DESCRIBE TABLE` under the hood), so the database always reflects the current state of the remote server. Each table is exposed as a [`Distributed`](/engines/table-engines/special/distributed) storage over an ad-hoc cluster built from the supplied addresses, which forwards `SELECT` and `INSERT` queries to the remote server.

This is handy for federating several ClickHouse clusters or for plugging a larger ClickHouse cluster into `clickhouse-local` or a smaller cluster.

## Creating a database {#creating-a-database}

<Tabs>
<Tab title="Remote" id="remote">

`Remote` connects over the plain TCP port (`tcp_port`, `9000` by default) when the port is omitted.

```sql
CREATE DATABASE remote_db
ENGINE = Remote('addresses_expr', 'database'[, 'user'[, 'password']]);
```

</Tab>
<Tab title="RemoteSecure" id="remote-secure">

`RemoteSecure` connects over a secure TLS connection using the secure TCP port (`tcp_port_secure`, `9440` by default) when the port is omitted.

```sql
CREATE DATABASE remote_db
ENGINE = RemoteSecure('addresses_expr', 'database'[, 'user'[, 'password']]);
```

</Tab>
</Tabs>

**Engine Parameters**

- `addresses_expr` — A remote server address or an expression that generates several addresses, in the form `host` or `host:port`. The address expression supports the same globbing patterns as the [`remote`](/sql-reference/table-functions/remote) table function (for example `{a,b,c}`, `{N..M}` and `{a|b}` to expand into multiple shards and replicas). When the port is omitted, `Remote` uses the plain TCP port (`tcp_port`, `9000` by default) and `RemoteSecure` uses the secure TCP port (`tcp_port_secure`, `9440` by default).
- `database` — The name of the database on the remote server.
- `user` — The remote user name. Optional, default: `default`.
- `password` — The remote user password. Optional, default: empty.

The addresses and credentials are stored in the database definition, so the password is hidden in `SHOW CREATE DATABASE`. As with the `remote` table function, an address that points to the current server is treated as a local shard: `SELECT` and `INSERT` are executed directly under the current user — who therefore needs the corresponding privileges on the underlying database and its tables — and the stored credentials are used only for genuinely remote servers. If the local replica of a shard does not have the database or a table, the lookup falls back to the remote replicas of the shard, like a [`Distributed`](/engines/table-engines/special/distributed) table does. In that case `SHOW CREATE TABLE` prints the effective fallback addresses (the local replicas stripped from their shards) instead of the configured ones, so the emitted `Remote(...)` table definition reconstructs the object that actually serves the queries.

When the address expression describes several shards, each proxy table reads from all of them, but the metadata — the list of the tables and their structure — is taken from an arbitrary shard (a local one is preferred), just like the [`remote`](/sql-reference/table-functions/remote) table function does, so that a listing costs a single query instead of one per shard. The shards of a cluster are therefore expected to serve the same set of tables; a table that only some of them have is served by a proxy whose queries then fail on the shards that do not have it. An `INSERT` into a table of a multi-shard database sends each row to a random shard (the proxy `Distributed` tables carry an implicit `rand()` sharding key); to pin the shard for a query, set [`insert_shard_id`](/reference/settings/session-settings/insert#insert_shard_id). The implicit key only distributes the inserted rows: for reading, the table behaves like a `Distributed` table without a sharding key (in particular, [`optimize_skip_unused_shards`](/reference/settings/session-settings/optimize-skip#optimize_skip_unused_shards) and [`force_optimize_skip_unused_shards`](/reference/settings/session-settings/force-optimize#force_optimize_skip_unused_shards) do not treat it as a shard-pruning key). `SHOW CREATE TABLE` includes the key in the emitted `Remote(...)` table definition, so a table recreated from it accepts multi-shard `INSERT` queries as well.

Named collections are supported as well:

```sql
CREATE DATABASE remote_db
ENGINE = Remote(my_named_collection, database = 'default');
```

## Notes {#notes}

- The engine is a read-through view of the remote server: `CREATE TABLE`, `DROP TABLE`, `ALTER` and similar DDL statements against the `Remote` database are not supported. Manage the schema on the remote server directly.
- Access rights are enforced on the remote server for the configured remote user, and locally by the usual privileges on the database and its tables.
- A table of a local shard that the user is not allowed to see is reported as missing rather than as forbidden, so a `Remote` database cannot be used to probe the table names of a local database the user has no privileges on. This applies to listing (`SHOW TABLES`, `EXISTS TABLE`) as well as to resolution (`DESCRIBE TABLE`, `SHOW CREATE TABLE`, `SELECT`), and such a table is not served through the remote replicas of its shard either: the fallback described above engages only when the local replica genuinely does not have the table.
- Listing the tables of a database that exists on the local replica of a shard also includes the tables that only the remote replicas of that shard have, so that `SHOW TABLES` and `system.tables` agree with `EXISTS TABLE`, `DESCRIBE TABLE` and `SELECT`, which fall back to those replicas. When none of the remote replicas answers, the list of the local replica is returned as it is, because it is already the answer of an available replica.
- If the remote server is unavailable, listing its tables (`SHOW TABLES`, `system.tables`) reports the connection error instead of an empty list of tables, as `EXISTS TABLE` and `SELECT` on the same database do. Note that a `SELECT` from `system.tables` covering all databases fails as well while such a database is unreachable.
- A `Remote` database may point to another `Remote` database on the same server. Listing and describing the tables of such a chain needs no privileges on the intermediate database — it holds neither data nor metadata of its own, and every hop already checks the caller's rights on the objects that it proxies in turn. Reading and writing the data, in contrast, needs `SELECT` / `INSERT` on every hop of the chain, because the query is really executed against the table of the intermediate database, exactly like for a `Distributed` table over another `Distributed` table. The visibility rule described above survives the chain: a table that the intermediate database hides from the caller is not served through the remote replicas of the outer database either. If the intermediate database on the local replica cannot reach its own target, the local replica of the outer shard cannot answer at all — exactly as if the replica itself were down — and the outer database falls back to the remote replicas of the shard.

## Example {#example}

Create a `Remote` database that points to the `system` database of a remote server and read from it:

```sql
CREATE DATABASE remote_system
ENGINE = Remote('127.0.0.1:9000', 'system', 'default', '');
```

```sql
SHOW TABLES FROM remote_system LIKE 'one';
```

```text
┌─name─┐
│ one  │
└──────┘
```

```sql
SELECT * FROM remote_system.one;
```

```text
┌─dummy─┐
│     0 │
└───────┘
```
)DOCS_MD";

    factory.registerDatabase(
        "Remote",
        create_fn,
        features,
        Documentation{
            .description = common_description,
            .syntax = "ENGINE = Remote('addresses_expr', 'database'[, 'user'[, 'password']])",
            .related = {"MySQL", "PostgreSQL"}});

    factory.registerDatabase(
        "RemoteSecure",
        create_fn,
        features,
        Documentation{
            .description = common_description,
            .syntax = "ENGINE = RemoteSecure('addresses_expr', 'database'[, 'user'[, 'password']])",
            .related = {"MySQL", "PostgreSQL"}});
}

}
