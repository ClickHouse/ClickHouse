#include <Interpreters/SystemCommands/SystemCommandHelpers.h>
#include <Interpreters/SystemCommands/SystemCommandsDropReplica.h>

#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/TablesDependencyGraph.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/extractZooKeeperPathFromReplicatedTableDef.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <base/scope_guard.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

#include <filesystem>

#include <fmt/ranges.h>

namespace fs = std::filesystem;


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int TABLE_WAS_NOT_DROPPED;
}

namespace
{

/// The form of `SYSTEM DROP REPLICA` / `SYSTEM DROP DATABASE REPLICA` without a database or a table
/// affects every database on the server that the command can target, so it requires `SYSTEM DROP REPLICA` for all of them.
/// Instead of silently skipping the databases the user has no access to (and possibly doing nothing at all),
/// check the permissions in advance and tell the user which databases they are missing the privilege for.
/// When there is nothing to target at all, the server-wide command is still a privileged operation
/// and must not succeed for a user without any privileges, so the global privilege is required in that case.
void checkAccessForDropWholeReplica(const ContextPtr & context, const Strings & target_databases, std::string_view query_name)
{
    auto access = context->getAccess();
    if (access->isGranted(AccessType::SYSTEM_DROP_REPLICA))
        return;

    if (target_databases.empty())
        context->checkAccess(AccessType::SYSTEM_DROP_REPLICA);

    std::vector<String> databases_without_access;
    for (const auto & database_name : target_databases)
        if (!access->isGranted(AccessType::SYSTEM_DROP_REPLICA, database_name))
            databases_without_access.emplace_back(database_name);

    if (!databases_without_access.empty())
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Access denied for {}. Not enough permissions to drop these databases: {}",
            query_name,
            fmt::join(databases_without_access, ", "));
}

/// `SYSTEM DROP REPLICA` and `SYSTEM DROP DATABASE REPLICA`.
class DropReplicaCommands : public SystemCommandContext
{
public:
    explicit DropReplicaCommands(const SystemCommandContext & command_context) : SystemCommandContext(command_context) {}

    void dropReplica(ASTSystemQuery & query);
    void dropDatabaseReplica(ASTSystemQuery & query);

private:
    bool dropStorageReplica(const String & query_replica, const StoragePtr & storage);
    void dropStorageReplicasFromDatabase(const String & query_replica, DatabasePtr database);
    DatabasePtr
    restoreDatabaseFromKeeperPath(const String & zookeeper_name, const String & zookeeper_path, const String & full_replica_name, const String & restoring_database_name);
    std::optional<String> getDetachedDatabaseFromKeeperPath(const ASTSystemQuery & query_);
};

void DropReplicaCommands::dropReplica(ASTSystemQuery & query)
{
    auto component_guard = Coordination::setCurrentComponent("InterpreterSystemQuery::dropReplica");
    if (query.replica.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name is empty");

    if (!table_id.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, table_id);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

        if (!dropStorageReplica(query.replica, table))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
    }
    else if (query.database)
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase());
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.getDatabase());
        dropStorageReplicasFromDatabase(query.replica, database);
        LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});
        Strings target_databases;
        for (const auto & elem : databases)
            target_databases.emplace_back(elem.first);
        checkAccessForDropWholeReplica(getContext(), target_databases, "SYSTEM DROP REPLICA");

        /// If we are here, then the user has the necessary access to drop the replica, continue with the operation.
        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            dropStorageReplicasFromDatabase(query.replica, database);
            LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (query.replica_zk_path.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path is empty");
    }
    else
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        String remote_replica_path = fs::path(query.replica_zk_path)  / "replicas" / query.replica;

        /// query.replica_zk_path keeps the legacy one-slash normalization so the drop below targets the
        /// same live znode that tables store (a full collapse here would regress dropping a remote replica
        /// of a table created with a trailing slash). For the self-protection comparison, collapse both
        /// sides so a self-drop spelled with extra trailing slashes still matches the local table.
        const String canonical_remote_replica_path
            = fs::path(zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(query.replica_zk_path, /*check_starts_with_slash*/ false))
            / "replicas" / query.replica;

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                /// The loaded storage if there is one, otherwise the lazy stand-in.
                const StoragePtr table = resolveStorageProxy(iterator->table());
                const auto * lazy_stand_in = dynamic_cast<const StorageProxy *>(table.get());
                String local_zookeeper_path;
                String local_replica_name;
                String local_zookeeper_name;
                if (lazy_stand_in && lazy_stand_in->isLazyStandIn())
                {
                    /// A lazily loaded table that nothing has accessed yet holds no session in ZooKeeper, so the
                    /// `is_active` check below would not catch it either. Skipping it is the destructive outcome,
                    /// and loading every such table defeats `lazy_load_tables`, so resolve its path from the stored
                    /// `CREATE TABLE` query the way loading the table would.
                    ASTPtr create_ast = database->tryGetCreateTableQuery(iterator->name(), getContext());
                    if (!create_ast)
                        continue; /// Dropped meanwhile.
                    create_ast = create_ast->clone();
                    auto & create_query = create_ast->as<ASTCreateQuery &>();
                    create_query.attach = true;
                    auto znode_info = extractZnodeInfoFromReplicatedTableDef(
                        create_query, table->getStorageID(), LoadingStrictnessLevel::ATTACH, getContext());
                    if (!znode_info)
                        continue;
                    local_zookeeper_path = znode_info->path;
                    local_replica_name = znode_info->replica_name;
                    local_zookeeper_name = znode_info->zookeeper_name;
                }
                else if (auto * storage_replicated = castStorage<StorageReplicatedMergeTree>(table, DeferredTable::Skip).get())
                {
                    local_zookeeper_path = storage_replicated->getZooKeeperPath();
                    local_replica_name = storage_replicated->getReplicaName();
                    local_zookeeper_name = storage_replicated->getZooKeeperName();
                }
                else
                {
                    continue;
                }

                /// getReplicaPath() is built from getZooKeeperPath(), which strips only a single trailing
                /// slash, so a table created from "/a///" metadata keeps "/a//replicas/..." and would slip
                /// past this guard against a query path canonicalized to "/a/replicas/...".
                const String local_replica_path
                    = fs::path(zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(local_zookeeper_path, /*check_starts_with_slash*/ false))
                    / "replicas" / local_replica_name;
                /// Match the keeper too: a table on a different keeper with the same path string is a
                /// different znode, so it must not block a drop targeting query.zk_name.
                if (local_replica_path == canonical_remote_replica_path && local_zookeeper_name == query.zk_name)
                    throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED,
                                    "There is a local table {}, which has the same table path in ZooKeeper. "
                                    "Please check the path in query. "
                                    "If you want to drop replica "
                                    "of this table, use `DROP TABLE` "
                                    "or `SYSTEM DROP REPLICA 'name' FROM TABLE db.table`",
                                    table->getStorageID().getNameForLogs());
            }
        }

        auto zookeeper = getContext()->getDefaultOrAuxiliaryZooKeeper(query.zk_name);

        bool looks_like_table_path = zookeeper->exists(query.replica_zk_path + "/replicas") ||
                                     zookeeper->exists(query.replica_zk_path + "/dropped");
        if (!looks_like_table_path)
            throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Specified path {} does not look like a table path",
                            query.replica_zk_path);

        if (zookeeper->exists(remote_replica_path + "/is_active"))
            throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Can't remove replica: {}, because it's active", query.replica);

        TableZnodeInfo info;
        info.path = query.replica_zk_path;
        info.replica_name = query.replica;
        StorageReplicatedMergeTree::dropReplica(zookeeper, info, log);
        LOG_INFO(log, "Dropped replica {}", remote_replica_path);
    }
}

bool DropReplicaCommands::dropStorageReplica(const String & query_replica, const StoragePtr & storage)
{
    /// Dropping a replica from Keeper is what the command asks for, so loading the table is warranted.
    auto * storage_replicated = castStorage<StorageReplicatedMergeTree>(storage, DeferredTable::Load).get();
    if (!storage_replicated)
        return false;

    const auto & replica_name = storage_replicated->getReplicaName();

    /// Do not allow to drop local replicas and active remote replicas
    if (query_replica == replica_name)
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED,
                        "We can't drop local replica, please use `DROP TABLE` if you want "
                        "to clean the data and drop this replica");

    storage_replicated->dropReplica(query_replica, log);
    LOG_TRACE(log, "Dropped replica {} of {}", query_replica, storage->getStorageID().getNameForLogs());

    return true;
}

void DropReplicaCommands::dropStorageReplicasFromDatabase(const String & query_replica, DatabasePtr database)
{
    for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
        dropStorageReplica(query_replica, iterator->table());

    LOG_TRACE(log, "Dropped storage replica from {} of database {}", query_replica, backQuoteIfNeed(database->getDatabaseName()));
}

DatabasePtr DropReplicaCommands::restoreDatabaseFromKeeperPath(
    const String & zookeeper_name, const String & zookeeper_path, const String & full_replica_name, const String & restoring_database_name)
{
    auto component_guard = Coordination::setCurrentComponent("InterpreterSystemQuery::restoreDatabaseFromKeeperPath");
    auto zookeeper = getContext()->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);

    String metadata_path = zookeeper_path + "/metadata";
    if (!zookeeper->exists(metadata_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database metadata keeper path does not exists: '{}'", metadata_path);

    if (zookeeper->getChildren(metadata_path).empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database replicas keeper path is empty: '{}", metadata_path);

    String replica_path = zookeeper_path + "/replicas/" + full_replica_name;
    if (!zookeeper->exists(replica_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database replica keeper path does not exists: '{}", replica_path);

    Strings escaped_table_names;
    escaped_table_names = zookeeper->getChildren(zookeeper_path + "/metadata");

    std::vector<String> paths_to_fetch;
    paths_to_fetch.reserve(escaped_table_names.size());

    for (const auto & table : escaped_table_names)
        paths_to_fetch.push_back(zookeeper_path + "/metadata/" + table);

    auto table_metadata = zookeeper->tryGet(paths_to_fetch);
    std::map<String, String> table_name_to_metadata;
    for (size_t i = 0; i < escaped_table_names.size(); ++i)
    {
        auto & res = table_metadata[i];
        if (res.error != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(res.error, fs::path(zookeeper_path) / "metadata");

        table_name_to_metadata.emplace(unescapeForFileName(escaped_table_names[i]), std::move(res.data));
    }
    String create_query = fmt::format("CREATE DATABASE `{}` ENGINE=Atomic", restoring_database_name);

    auto create_ctx = Context::createCopy(Context::getGlobalContextInstance());
    create_ctx->makeQueryContext();
    create_ctx->setCurrentQueryId({});
    executeQuery(create_query, create_ctx, QueryFlags{.internal = true});

    TablesDependencyGraph tables_dependencies("Memory (" + restoring_database_name + ")");
    LOG_TEST(log, "table_name_to_metadata size={}", table_name_to_metadata.size());

    for (const auto & [table_name, create_table_query] : table_name_to_metadata)
    {
        /// Note that table_name could contain a dot inside (e.g. .inner.1234-1234-1234-1234)
        /// And QualifiedTableName::parseFromString doesn't handle this.
        auto qualified_name = QualifiedTableName{.database = restoring_database_name, .table = table_name};
        auto query_ast = DatabaseReplicated::parseQueryFromMetadataInZooKeeper(
            getContext(),
            /*database_name_=*/restoring_database_name,
            /*zookeeper_path_=*/zookeeper_path,
            /*node_name=*/table_name,
            /*query=*/create_table_query);
        LOG_TEST(
            log, "table_name {}, create_table_query: {}, query_ast: {}", table_name, create_table_query, query_ast->formatForLogging());

        const ASTCreateQuery * create_query_ast = query_ast->as<ASTCreateQuery>();
        chassert(create_query_ast);

        if (create_query_ast->is_materialized_view)
        {
            LOG_TRACE(log, "Table {} is a materialized view, skip restoring", table_name);
            continue;
        }

        if (!create_query_ast->storage)
        {
            LOG_TRACE(log, "No storage defined in table create statement: {}, '{}', skip restoring", table_name, create_table_query);
            continue;
        }
        if (!create_query_ast->storage->engine)
        {
            LOG_TRACE(log, "No engine defined in create statement: {}, '{}', skip restoring", table_name, create_table_query);
            continue;
        }

        const String & engine_name = create_query_ast->storage->engine->name;
        if (!engine_name.starts_with("Replicated") || !engine_name.ends_with("MergeTree"))
        {
            LOG_TRACE(log, "Engine of the table '{}' is not ReplicatedMergeTree: '{}', skip restoring", table_name, engine_name);
            continue;
        }

        tables_dependencies.addDependencies(
            qualified_name,
            getDependenciesFromCreateQuery(getContext()->getGlobalContext(), qualified_name, query_ast, getContext()->getCurrentDatabase())
                .dependencies);
    }

    auto make_create_context = [this, &restoring_database_name]()
    {
        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->setDDLOrOnClusterInternal(true);
        query_context->setCurrentDatabase(restoring_database_name);
        query_context->setCurrentQueryId("");

        /// The CREATE queries below come from metadata a Replicated database stored in Keeper, so they
        /// must be accepted as they are: they re-derive tables that exist elsewhere.
        query_context->setRecoveryFromStoredMetadata(true);

        /// We will execute some CREATE queries for recovery (not ATTACH queries),
        /// so we need to allow experimental features that can be used in a CREATE query
        enableAllExperimentalSettings(query_context);

        /// We apply the flatten_nested setting after writing the CREATE query to the DDL log,
        /// but before writing metadata to ZooKeeper. So we have to apply the setting on secondary replicas, but not in recovery mode.
        /// Set it to false, so it will do nothing on recovery. The metadata in ZooKeeper should be used as is.
        /// Same for data_type_default_nullable.
        query_context->setSetting("flatten_nested", false);
        query_context->setSetting("data_type_default_nullable", false);

        return query_context;
    };

    auto tables_to_create_by_level = tables_dependencies.getTablesSplitByDependencyLevel();
    for (const auto & tables_to_create : tables_to_create_by_level)
    {
        for (const auto & storage_id : tables_to_create)
        {
            auto table_name = storage_id.getTableName();

            auto metadata_it = table_name_to_metadata.find(table_name);
            if (metadata_it == table_name_to_metadata.end())
            {
                /// getTablesSortedByDependency() may return some not existing tables or tables from other databases
                LOG_WARNING(
                    log,
                    "Got table name {} when resolving table dependencies, "
                    "but database {} does not have metadata for that table. Ignoring it",
                    storage_id.getNameForLogs(),
                    restoring_database_name);
                continue;
            }
            const auto & create_query_string = metadata_it->second;
            auto query_ast = DatabaseReplicated::parseQueryFromMetadataInZooKeeper(
                getContext(),
                /*database_name_=*/restoring_database_name,
                /*zookeeper_path_=*/zookeeper_path,
                /*node_name=*/table_name,
                /*query=*/create_query_string);
            auto create_query_context = make_create_context();

            {
                SelectIntersectExceptQueryVisitor::Data data{create_query_context->getSettingsRef()[Setting::intersect_default_mode], create_query_context->getSettingsRef()[Setting::except_default_mode]};
                SelectIntersectExceptQueryVisitor{data}.visit(query_ast);
            }
            {
                NormalizeSelectWithUnionQueryVisitor::Data data{create_query_context->getSettingsRef()[Setting::union_default_mode]};
                NormalizeSelectWithUnionQueryVisitor{data}.visit(query_ast);
            }

            LOG_INFO(log, "Restoring {}", query_ast->formatForLogging());
            InterpreterCreateQuery(query_ast, create_query_context).execute();
        }
    }
    LOG_INFO(log, "All tables are restored successfully");

    auto database = DatabaseCatalog::instance().getDatabase(restoring_database_name);
    chassert(database);
    return database;
}

std::optional<String> DropReplicaCommands::getDetachedDatabaseFromKeeperPath(const ASTSystemQuery & query_)
{
    auto metadata_dir_path = DatabaseCatalog::getMetadataDirPath();
    auto default_db_disk = getContext()->getDatabaseDisk();
    for (const auto it = default_db_disk->iterateDirectory(metadata_dir_path); it->isValid(); it->next())
    {
        auto sub_path = fs::path(it->path());
        if (!default_db_disk->existsFile(sub_path))
            continue;

        String db_name = sub_path.filename().string();
        if (sub_path.extension() == ".sql")
            db_name = sub_path.stem();

        auto buf = default_db_disk->readFile(sub_path, getContext()->getReadSettings());
        std::string query;
        readStringUntilEOF(query, *buf);
        ParserCreateQuery parser;
        auto ast = parseQuery(
            parser,
            query,
            "",
            0,
            getContext()->getSettingsRef()[Setting::max_parser_depth],
            getContext()->getSettingsRef()[Setting::max_parser_backtracks]);

        const auto * create_ast = ast->as<ASTCreateQuery>();
        auto * engine_define = create_ast->storage;
        if (!engine_define)
            continue;
        const ASTFunction * engine = engine_define->engine;
        if (!engine || engine->name != "Replicated")
            continue;

        auto & arguments = engine->arguments->children;
        if (!engine->arguments || engine->arguments->children.size() != 3)
            continue;

        String engine_zookeeper_path = safeGetLiteralValue<String>(arguments[0], "Replicated");
        String shard_name = safeGetLiteralValue<String>(arguments[1], "Replicated");
        String replica_name = safeGetLiteralValue<String>(arguments[2], "Replicated");

        Macros::MacroExpansionInfo info;
        engine_zookeeper_path = getContext()->getMacros()->expand(engine_zookeeper_path, info);

        info.level = 0;
        shard_name = getContext()->getMacros()->expand(shard_name, info);

        info.level = 0;
        replica_name = getContext()->getMacros()->expand(replica_name, info);

        if (engine_zookeeper_path.empty())
            continue;

        /// The raw engine argument may carry an auxiliary keeper prefix ("aux:/path"), so extract the
        /// keeper name and fully collapse the path, then match on BOTH. Comparing the raw argument against
        /// the query path would both false-match a default-keeper database at the same path (blocking a
        /// valid auxiliary-keeper drop) and never match a detached auxiliary-keeper database (silently
        /// failing this guard). query_.replica_zk_path keeps the legacy one-slash normalization for the
        /// actual drop, so collapse it here too to compare canonical forms.
        String engine_zookeeper_name = zkutil::extractZooKeeperName(engine_zookeeper_path);
        engine_zookeeper_path = zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(engine_zookeeper_path, /*check_starts_with_slash*/ false);
        const String canonical_query_zk_path
            = zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(query_.replica_zk_path, /*check_starts_with_slash*/ false);

        if (engine_zookeeper_name != query_.zk_name || engine_zookeeper_path != canonical_query_zk_path)
            continue;

        String full_replica_name
            = query_.shard.empty() ? query_.replica : DatabaseReplicated::getFullReplicaName(query_.shard, query_.replica);
        if (DatabaseReplicated::getFullReplicaName(shard_name, replica_name) != full_replica_name)
            continue;

        return db_name;
    }
    return std::nullopt;
}

void DropReplicaCommands::dropDatabaseReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name is empty");

    auto component_guard = Coordination::setCurrentComponent("InterpreterSystemQuery::dropDatabaseReplica");
    const String full_replica_name
        = query.shard.empty() ? query.replica : DatabaseReplicated::getFullReplicaName(query.shard, query.replica);
    /// query.replica_zk_path keeps the legacy one-slash normalization so DatabaseReplicated::dropReplica
    /// below targets the same live znode a Replicated database stores. Collapse it here only for the
    /// self-protection comparison, so a self-drop spelled with extra trailing slashes still matches.
    const fs::path query_replica_zk_path
        = query.replica_zk_path.empty()
        ? fs::path{}
        : fs::path(zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(query.replica_zk_path, /*check_starts_with_slash*/ false));
    auto check_not_local_replica = [](const DatabaseReplicated * replicated, const String & full_replica_name_,
                                       const fs::path & query_replica_zk_path_, const String & query_zk_name_)
    {
        /// When a ZKPATH is given, a database on a different keeper (or path) is a different znode and must
        /// not block a drop targeting query_zk_name_. Canonicalize the database path the same way we
        /// canonicalize query_replica_zk_path_: getZooKeeperPath() only strips a single trailing slash, so a
        /// database created from "/a///" metadata keeps "/a/" and would otherwise slip past this guard.
        const String replicated_zk_path
            = zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(replicated->getZooKeeperPath(), /*check_starts_with_slash*/ false);
        if (!query_replica_zk_path_.empty()
            && (fs::path(replicated_zk_path) != query_replica_zk_path_
                || replicated->getZooKeeperName() != query_zk_name_))
            return;
        if (replicated->getFullReplicaName() != full_replica_name_)
            return;

        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "There is a local database {}, which has the same path in ZooKeeper "
                        "and the same replica name. Please check the path in query. "
                        "If you want to drop replica of this database, use `DROP DATABASE`", replicated->getDatabaseName());
    };

    if (query.database)
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase());
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.getDatabase());
        if (auto * replicated = dynamic_cast<DatabaseReplicated *>(database.get()))
        {
            check_not_local_replica(replicated, full_replica_name, query_replica_zk_path, query.zk_name);
            if (query.with_tables)
                dropStorageReplicasFromDatabase(query.replica, database);
            DatabaseReplicated::dropReplica(
                replicated, replicated->getZooKeeperName(), replicated->getZooKeeperPath(), query.shard, query.replica, /*throw_if_noop*/ true);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database {} is not Replicated, cannot drop replica", query.getDatabase());
        LOG_TRACE(log, "Dropped replica {} of Replicated database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});

        /// Only `Replicated` databases are affected by this command, so only they require the privilege:
        /// a user must not be denied because of unrelated databases the command would never touch.
        Strings target_databases;
        for (const auto & elem : databases)
            if (dynamic_cast<DatabaseReplicated *>(elem.second.get()))
                target_databases.emplace_back(elem.first);
        checkAccessForDropWholeReplica(getContext(), target_databases, "SYSTEM DROP DATABASE REPLICA");

        /// If we are here, then the user has the necessary access to drop the replica, continue with the operation.
        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            auto * replicated = dynamic_cast<DatabaseReplicated *>(database.get());
            if (!replicated)
                continue;

            check_not_local_replica(replicated, full_replica_name, query_replica_zk_path, query.zk_name);
            if (query.with_tables)
                dropStorageReplicasFromDatabase(query.replica, database);
            DatabaseReplicated::dropReplica(
                replicated, replicated->getZooKeeperName(), replicated->getZooKeeperPath(), query.shard, query.replica, /*throw_if_noop*/ false);
            LOG_TRACE(log, "Dropped replica {} of Replicated database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (query.replica_zk_path.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path is empty");
    }
    else
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
            if (auto * replicated = dynamic_cast<DatabaseReplicated *>(elem.second.get()))
                check_not_local_replica(replicated, full_replica_name, query_replica_zk_path, query.zk_name);

        if (query.with_tables)
        {
            auto detached_database_name = getDetachedDatabaseFromKeeperPath(query);
            if (detached_database_name)
                throw Exception(
                    ErrorCodes::TABLE_WAS_NOT_DROPPED,
                    "There is a detached database {}, which has the same path in ZooKeeper "
                    "and the same replica name. Please check the path in query. "
                    "If you want to drop replica of this database, reattach it with `ATTACH DATABASE`, then drop it with `DROP DATABASE`",
                    *detached_database_name);

            String restoring_database_name = RESTORING_DATABASE_NAME_FOR_TABLE_DROPPING_PREFIX + getRandomASCIIString(32);
            SCOPE_EXIT({
                auto drop_ctx = Context::createCopy(Context::getGlobalContextInstance());
                drop_ctx->makeQueryContext();
                drop_ctx->setCurrentQueryId(toString(UUIDHelpers::generateV4()));
                String drop_query = fmt::format("DROP DATABASE IF EXISTS `{}` SYNC", restoring_database_name);
                executeQuery(drop_query, drop_ctx);
            });
            auto database = restoreDatabaseFromKeeperPath(
                /*zookeeper_name=*/query.zk_name,
                /*zookeeper_path=*/query.replica_zk_path,
                /*full_replica_name=*/full_replica_name,
                /*restoring_database_name=*/restoring_database_name);
            if (database)
            {
                dropStorageReplicasFromDatabase(query.replica, database);
                database.reset();
            }
        }

        DatabaseReplicated::dropReplica(nullptr, query.zk_name, query.replica_zk_path, query.shard, query.replica, /*throw_if_noop*/ true);
        LOG_INFO(log, "Dropped replica {} of Replicated database with path {}", query.replica, query.full_replica_zk_path);
    }
}

}

void registerSystemCommandsDropReplica(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    /// For the whole-server form (no database and no table) this requires the global privilege.
    /// This is intentional: the initiator does not know which databases exist on the other hosts of the cluster,
    /// so it cannot narrow the check to the affected databases as `checkAccessForDropWholeReplica` does locally.
    const auto access_on_cluster = tableAccessOnCluster(AccessType::SYSTEM_DROP_REPLICA);

    factory.registerCommand(Type::DROP_REPLICA, access_on_cluster, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        DropReplicaCommands(context).dropReplica(query);
        return BlockIO{};
    });

    factory.registerCommand(Type::DROP_DATABASE_REPLICA, access_on_cluster, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        DropReplicaCommands(context).dropDatabaseReplica(query);
        return BlockIO{};
    });
}

}
