#include <filesystem>
#include <memory>

#include <Core/Defines.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/TablesLoader.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/CurrentMetrics.h>
#include <Common/PoolId.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <boost/algorithm/string/replace.hpp>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_database_ordinary;
    extern const SettingsBool fsync_metadata;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsSetOperationMode union_default_mode;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString storage_policy;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_replica_name;
    extern const ServerSettingsString default_replica_path;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int NOT_IMPLEMENTED;
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
}

namespace DatabaseMetadataDiskSetting
{
extern const DatabaseMetadataDiskSettingsString disk;
}


static constexpr const char * const CONVERT_TO_REPLICATED_FLAG_NAME = "convert_to_replicated";

DatabaseOrdinary::DatabaseOrdinary(
    const String & name_, const String & metadata_path_, ContextPtr context_, DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseOrdinary(
          name_,
          metadata_path_,
          std::filesystem::path("data") / escapeForFileName(name_) / "",
          "DatabaseOrdinary (" + name_ + ")",
          context_,
          database_metadata_disk_settings_)
{
}

DatabaseOrdinary::DatabaseOrdinary(
    const String & name_,
    const String & metadata_path_,
    const String & data_path_,
    const String & logger,
    ContextPtr context_,
    DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseOnDisk(name_, metadata_path_, data_path_, logger, context_)
    , database_metadata_disk_settings(database_metadata_disk_settings_)
{
    if (!database_metadata_disk_settings[DatabaseMetadataDiskSetting::disk].value.empty())
        metadata_disk_ptr = getContext()->getDisk(database_metadata_disk_settings[DatabaseMetadataDiskSetting::disk].value);
    else
        metadata_disk_ptr = getContext()->getDatabaseDisk();

    LOG_INFO(log, "Metadata disk {}, path {}", metadata_disk_ptr->getName(), metadata_disk_ptr->getPath());
}

void DatabaseOrdinary::loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel)
{
    // Because it supportsLoadingInTopologicalOrder, we don't need this loading method.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

static void checkReplicaPathExists(ASTCreateQuery & create_query, ContextPtr local_context)
{
    Macros::MacroExpansionInfo info;
    StorageID table_id = StorageID(create_query.getDatabase(), create_query.getTable(), create_query.uuid);
    info.table_id = table_id;
    info.expand_special_macros_only = false;

    const auto & server_settings = local_context->getServerSettings();
    String replica_path = server_settings[ServerSetting::default_replica_path];
    String zookeeper_path = local_context->getMacros()->expand(replica_path, info);
    if (local_context->getZooKeeper()->exists(zookeeper_path))
        throw Exception(
            ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER,
            "Found existing ZooKeeper path {} while trying to convert table {} to replicated. Table will not be converted.",
            zookeeper_path, backQuote(table_id.getFullTableName())
        );
}

void DatabaseOrdinary::setMergeTreeEngine(ASTCreateQuery & create_query, ContextPtr local_context, bool replicated)
{
    auto * storage = create_query.storage;
    auto args = std::make_shared<ASTExpressionList>();
    auto engine = std::make_shared<ASTFunction>();
    String engine_name;

    if (replicated)
    {
        const auto & server_settings = local_context->getServerSettings();
        String replica_path = server_settings[ServerSetting::default_replica_path];
        String replica_name = server_settings[ServerSetting::default_replica_name];

        args->children.push_back(std::make_shared<ASTLiteral>(replica_path));
        args->children.push_back(std::make_shared<ASTLiteral>(replica_name));

        /// Add old engine's arguments
        if (storage->engine->arguments)
        {
            for (size_t i = 0; i < storage->engine->arguments->children.size(); ++i)
                args->children.push_back(storage->engine->arguments->children[i]->clone());
        }

        engine_name = "Replicated" + storage->engine->name;
    }
    else
    {
        /// Add old engine's arguments without first two
        if (storage->engine->arguments)
        {
            for (size_t i = 2; i < storage->engine->arguments->children.size(); ++i)
                args->children.push_back(storage->engine->arguments->children[i]->clone());
        }

        engine_name = storage->engine->name.substr(strlen("Replicated"));
    }

    /// Set new engine for the old query
    engine->name = engine_name;
    engine->arguments = args;
    create_query.storage->set(create_query.storage->engine, engine->clone());
}

String DatabaseOrdinary::getConvertToReplicatedFlagPath(const String & name, bool tableStarted)
{
    fs::path data_path;
    if (!tableStarted)
    {
        auto create_query = tryGetCreateTableQuery(name, getContext());
        data_path = getTableDataPath(create_query->as<ASTCreateQuery &>());
    }
    else
        data_path = getTableDataPath(name);

    return (data_path / CONVERT_TO_REPLICATED_FLAG_NAME);
}

void DatabaseOrdinary::convertMergeTreeToReplicatedIfNeeded(ASTPtr ast, const QualifiedTableName & qualified_name, const String & file_name)
{
    auto db_disk = getDisk();

    fs::path path(getMetadataPath());
    fs::path file_path(file_name);
    fs::path full_path = path / file_path;

    auto & create_query = ast->as<ASTCreateQuery &>();

    if (!create_query.storage || !create_query.storage->engine->name.ends_with("MergeTree") || create_query.storage->engine->name.starts_with("Replicated") || create_query.storage->engine->name.starts_with("Shared"))
        return;

    /// Get table's storage policy
    MergeTreeSettings default_settings = getContext()->getMergeTreeSettings();
    auto policy = getContext()->getStoragePolicy(default_settings[MergeTreeSetting::storage_policy]);
    if (auto * query_settings = create_query.storage->settings)
        if (Field * policy_setting = query_settings->changes.tryGet("storage_policy"))
            policy = getContext()->getStoragePolicy(policy_setting->safeGet<String>());

    auto convert_to_replicated_flag_path = getConvertToReplicatedFlagPath(qualified_name.table, false);

    auto storage_disks = policy->getDisks();
    auto checking_disk = storage_disks.empty() ? getDisk() : storage_disks[0];
    if (!checking_disk->existsFile(convert_to_replicated_flag_path))
        return;

    if (getUUID() == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Table engine conversion to replicated is supported only for Atomic databases. Convert your database engine to Atomic first.");

    LOG_INFO(log, "Found {} flag for table {}. Will try to change it's engine in metadata to replicated.", CONVERT_TO_REPLICATED_FLAG_NAME, backQuote(qualified_name.getFullName()));

    checkReplicaPathExists(create_query, getContext());
    setMergeTreeEngine(create_query, getContext(), /*replicated*/ true);

    /// Write changes to metadata
    String table_metadata_path = full_path;
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement = getObjectDefinitionFromCreateQuery(ast);
    writeMetadataFile(
        db_disk,
        /*file_path=*/table_metadata_tmp_path,
        /*content=*/statement,
        /*fsync_metadata=*/getContext()->getSettingsRef()[Setting::fsync_metadata]);

    db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);

    LOG_INFO(
        log,
        "Engine of table {} is set to replicated in metadata. Not removing {} flag until table is loaded and metadata in zookeeper is restored.",
        backQuote(qualified_name.getFullName()),
        CONVERT_TO_REPLICATED_FLAG_NAME
    );
}

void DatabaseOrdinary::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    auto db_disk = getDisk();

    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;
    size_t prev_total_materialized_views = metadata.total_materialized_views;

    auto process_metadata = [&metadata, is_startup, local_context, db_disk, this](const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast
                = parseQueryFromMetadata(log, local_context, db_disk, full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                FunctionNameNormalizer::visit(ast.get());
                auto * create_query = ast->as<ASTCreateQuery>();
                /// NOTE No concurrent writes are possible during database loading
                create_query->setDatabase(TSA_SUPPRESS_WARNING_FOR_READ(database_name));

                /// Even if we don't load the table we can still mark the uuid of it as taken.
                if (create_query->uuid != UUIDHelpers::Nil)
                {
                    /// A bit tricky way to distinguish ATTACH DATABASE and server startup (actually it's "force_attach" flag).
                    if (is_startup)
                    {
                        /// Server is starting up. Lock UUID used by permanently detached table.
                        DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);
                    }
                    else if (!DatabaseCatalog::instance().hasUUIDMapping(create_query->uuid))
                    {
                        /// It's ATTACH DATABASE. UUID for permanently detached table must be already locked.
                        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
                        if (getEngineName() != "MaterializedPostgreSQL")
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create_query->uuid);
                    }
                }

                if (db_disk->existsFile(full_path.string() + detached_suffix))
                {
                    const std::string table_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));
                    LOG_DEBUG(log, "Skipping permanently detached table {}.", backQuote(table_name));

                    std::lock_guard lock(mutex);
                    permanently_detached_tables.push_back(table_name);

                    const auto detached_table_name = create_query->getTable();

                    snapshot_detached_tables.emplace(
                        detached_table_name,
                        SnapshotDetachedTable{
                            .database = create_query->getDatabase(),
                            .table = detached_table_name,
                            .uuid = create_query->uuid,
                            .metadata_path = getObjectMetadataPath(detached_table_name),
                            .is_permanently = true});

                    LOG_TRACE(log, "Add permanently detached table {} to system.detached_tables", detached_table_name);
                    return;
                }

                QualifiedTableName qualified_name{TSA_SUPPRESS_WARNING_FOR_READ(database_name), create_query->getTable()};

                convertMergeTreeToReplicatedIfNeeded(ast, qualified_name, file_name);

                NormalizeSelectWithUnionQueryVisitor::Data data{local_context->getSettingsRef()[Setting::union_default_mode]};
                NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);
                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{full_path.string(), ast};
                metadata.total_dictionaries += create_query->is_dictionary;
                metadata.total_materialized_views += create_query->is_materialized_view;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    iterateMetadataFiles(process_metadata);

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t materialized_views_in_database = metadata.total_materialized_views - prev_total_materialized_views;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(log, "Metadata processed, database {} has {} tables, {} dictionaries and {} materialized views in total.",
             TSA_SUPPRESS_WARNING_FOR_READ(database_name), tables_in_database, dictionaries_in_database, materialized_views_in_database);
}

void DatabaseOrdinary::loadTableFromMetadata(
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    assert(name.database == TSA_SUPPRESS_WARNING_FOR_READ(database_name));
    const auto & query = ast->as<const ASTCreateQuery &>();

    LOG_TRACE(log, "Loading table {}", name.getFullName());

    constexpr size_t max_tries = 3;
    size_t tries = 0;
    time_t sleep_time = 1;

    while (true)
    {
        try
        {
            auto [table_name, table] = createTableFromAST(
                query,
                name.database,
                getTableDataPath(query),
                local_context,
                mode);

            attachTable(local_context, table_name, table, getTableDataPath(query));
            return;
        }
        catch (Coordination::Exception & e)
        {
            e.addMessage(
                "Cannot attach table " + backQuote(name.database) + "." + backQuote(query.getTable()) + " from metadata file " + file_path
                + " from query " + query.formatForErrorMessage());

            if (!Coordination::isHardwareError(e.code))
                throw;
            tryLogCurrentException(log);
            sleepForSeconds(sleep_time);
            sleep_time *= 2;
            ++tries;
            if (tries > max_tries)
                throw;
        }
        catch (Exception & e)
        {
            e.addMessage(
                "Cannot attach table " + backQuote(name.database) + "." + backQuote(query.getTable()) + " from metadata file " + file_path
                + " from query " + query.formatForErrorMessage());
            throw;
        }
    }
}

LoadTaskPtr DatabaseOrdinary::loadTableFromMetadataAsync(
    AsyncLoader & async_loader,
    LoadJobSet load_after,
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    std::scoped_lock lock(mutex);
    auto job = makeLoadJob(
        std::move(load_after),
        TablesLoaderBackgroundLoadPoolId,
        fmt::format("load table {}", name.getFullName()),
        [this, local_context, file_path, name, ast, mode] (AsyncLoader &, const LoadJobPtr &)
        {
            loadTableFromMetadata(local_context, file_path, name, ast, mode);
        });

    return load_table[name.table] = makeLoadTask(async_loader, {job});
}

void DatabaseOrdinary::restoreMetadataAfterConvertingToReplicated(StoragePtr table, const QualifiedTableName & name)
{
    auto * rmt = table->as<StorageReplicatedMergeTree>();
    if (!rmt)
        return;

    auto convert_to_replicated_flag_path = getConvertToReplicatedFlagPath(name.table, true);

    auto storage_disks = table->getStoragePolicy()->getDisks();
    auto checking_disk = storage_disks.empty() ? getDisk() : storage_disks[0];
    if (!checking_disk->existsFile(convert_to_replicated_flag_path))
        return;

    checking_disk->removeFileIfExists(convert_to_replicated_flag_path);
    LOG_INFO
    (
        log,
        "Removing convert to replicated flag for {}.",
        backQuote(name.getFullName())
    );

    auto has_metadata = rmt->hasMetadataInZooKeeper();
    if (!has_metadata.has_value())
    {
        LOG_WARNING
        (
            log,
            "No connection to ZooKeeper, can't restore metadata for {} in ZooKeeper after conversion. Run SYSTEM RESTORE REPLICA while connected to ZooKeeper.",
            backQuote(name.getFullName())
        );
    }
    else if (*has_metadata)
    {
        LOG_INFO
        (
            log,
            "Table {} already has metatada in ZooKeeper.",
            backQuote(name.getFullName())
        );
    }
    else
    {
        rmt->restoreMetadataInZooKeeper(/* zookeeper_retries_info = */ {}, false);
        LOG_INFO
        (
            log,
            "Metadata in ZooKeeper for {} is restored.",
            backQuote(name.getFullName())
        );
    }
}

LoadTaskPtr DatabaseOrdinary::startupTableAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    const QualifiedTableName & name,
    LoadingStrictnessLevel /*mode*/)
{
    std::scoped_lock lock(mutex);

    /// Initialize progress indication on the first call
    if (total_tables_to_startup == 0)
    {
        total_tables_to_startup = tables.size();
        startup_watch.restart();
    }

    auto job = makeLoadJob(
        std::move(startup_after),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup table {}", name.getFullName()),
        [this, name] (AsyncLoader &, const LoadJobPtr &)
        {
            if (auto table = tryGetTableNoWait(name.table))
            {
                /// Since startup() method can use physical paths on disk we don't allow any exclusive actions (rename, drop so on)
                /// until startup finished.
                auto table_lock_holder = table->lockForShare(RWLockImpl::NO_QUERY, getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
                table->startup();

                /// If table is ReplicatedMergeTree after conversion from MergeTree,
                /// it is in readonly mode due to metadata in zookeeper missing.
                restoreMetadataAfterConvertingToReplicated(table, name);

                logAboutProgress(log, ++tables_started, total_tables_to_startup, startup_watch);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {}.{} doesn't exist during startup",
                    backQuote(name.database), backQuote(name.table));
        });

    return startup_table[name.table] = makeLoadTask(async_loader, {job});
}

LoadTaskPtr DatabaseOrdinary::startupDatabaseAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    LoadingStrictnessLevel /*mode*/)
{
    auto job = makeLoadJob(
        std::move(startup_after),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup Ordinary database {}", getDatabaseName()),
        ignoreDependencyFailure,
        [] (AsyncLoader &, const LoadJobPtr &)
        {
            // NOTE: this job is no-op, but it is required for correct dependency handling
            // 1) startup should be done after tables loading
            // 2) load or startup errors for tables should not lead to not starting up the whole database
        });
    std::scoped_lock lock(mutex);
    return startup_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseOrdinary::waitTableStarted(const String & name) const
{
    /// Prioritize jobs (load and startup the table) to be executed in foreground pool and wait for them synchronously
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        if (auto it = startup_table.find(name); it != startup_table.end())
            task = it->second;
    }

    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task);
}

void DatabaseOrdinary::waitDatabaseStarted() const
{
    /// Prioritize load and startup of all tables and database itself and wait for them synchronously
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task);
}

void DatabaseOrdinary::stopLoading()
{
    std::unordered_map<String, LoadTaskPtr> stop_load_table;
    std::unordered_map<String, LoadTaskPtr> stop_startup_table;
    LoadTaskPtr stop_startup_database;
    {
        std::scoped_lock lock(mutex);
        stop_load_table.swap(load_table);
        stop_startup_table.swap(startup_table);
        stop_startup_database.swap(startup_database_task);
    }

    // Cancel pending tasks and wait for currently running tasks
    // Note that order must be backward of how it was created to make sure no dependent task is run after waiting for current task
    stop_startup_database.reset();
    stop_startup_table.clear();
    stop_load_table.clear();
}

DatabaseTablesIteratorPtr DatabaseOrdinary::getTablesIterator(ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    if (!skip_not_loaded)
    {
        // Wait for every table (matching the filter) to be loaded and started up before we make the snapshot.
        // It is important, because otherwise table might be:
        //  - not attached and thus will be missed in the snapshot;
        //  - not started, which is not good for DDL operations.
        LoadTaskPtrs tasks_to_wait;
        {
            std::lock_guard lock(mutex);
            if (!filter_by_table_name)
                tasks_to_wait.reserve(startup_table.size());
            for (const auto & [table_name, task] : startup_table)
                if (!filter_by_table_name || filter_by_table_name(table_name))
                    tasks_to_wait.emplace_back(task);
        }
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), tasks_to_wait);
    }
    return DatabaseWithOwnTablesBase::getTablesIterator(local_context, filter_by_table_name, skip_not_loaded);
}

DatabaseDetachedTablesSnapshotIteratorPtr DatabaseOrdinary::getDetachedTablesIterator(
    ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    return DatabaseWithOwnTablesBase::getDetachedTablesIterator(local_context, filter_by_table_name, skip_not_loaded);
}

Strings DatabaseOrdinary::getAllTableNames(ContextPtr) const
{
    std::set<String> unique_names;
    {
        std::lock_guard lock(mutex);
        for (const auto & [table_name, _] : tables)
            unique_names.emplace(table_name);
        // Not yet loaded table are not listed in `tables`, so we have to add table names from tasks
        for (const auto & [table_name, _] : startup_table)
            unique_names.emplace(table_name);
    }
    return {unique_names.begin(), unique_names.end()};
}

void DatabaseOrdinary::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    auto db_disk = getDisk();
    waitDatabaseStarted();

    String table_name = table_id.table_name;

    /// Read the definition of the table and replace the necessary parts with new ones.
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement = readMetadataFile(db_disk, table_metadata_path);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        statement.data(),
        statement.data() + statement.size(),
        "in file " + table_metadata_path,
        0,
        local_context->getSettingsRef()[Setting::max_parser_depth],
        local_context->getSettingsRef()[Setting::max_parser_backtracks]);

    applyMetadataChangesToCreateQuery(ast, metadata, local_context);

    statement = getObjectDefinitionFromCreateQuery(ast);
    auto ref_dependencies = getDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast, local_context->getCurrentDatabase());
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    DatabaseCatalog::instance().checkTableCanBeAddedWithNoCyclicDependencies(table_id.getQualifiedName(), ref_dependencies.dependencies, loading_dependencies);
    writeMetadataFile(
        db_disk,
        /*file_path=*/table_metadata_tmp_path,
        /*content=*/statement,
        /*fsync_metadata=*/getContext()->getSettingsRef()[Setting::fsync_metadata]);

    /// The create query of the table has been just changed, we need to update dependencies too.
    DatabaseCatalog::instance().updateDependencies(table_id, ref_dependencies.dependencies, loading_dependencies, ref_dependencies.mv_from_dependency ? TableNamesSet{ref_dependencies.mv_from_dependency->getQualifiedName()} : TableNamesSet{});

    commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, local_context);
}

void DatabaseOrdinary::commitAlterTable(const StorageID &, const String & table_metadata_tmp_path, const String & table_metadata_path, const String & /*statement*/, ContextPtr /*query_context*/)
{
    auto db_disk = getDisk();
    try
    {
        /// rename atomically replaces the old file with the new one.
        db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(table_metadata_tmp_path);
        throw;
    }
}

void registerDatabaseOrdinary(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        if (!args.create_query.attach && !args.context->getSettingsRef()[Setting::allow_deprecated_database_ordinary])
            throw Exception(
                ErrorCodes::UNKNOWN_DATABASE_ENGINE,
                "Ordinary database engine is deprecated (see also allow_deprecated_database_ordinary setting)");

        args.context->addWarningMessageAboutDatabaseOrdinary(args.database_name);

        DatabaseMetadataDiskSettings database_metadata_disk_settings;
        auto * engine_define = args.create_query.storage;
        chassert(engine_define);
        database_metadata_disk_settings.loadFromQuery(*engine_define, args.context, args.create_query.attach);

        return make_shared<DatabaseOrdinary>(args.database_name, args.metadata_path, args.context, database_metadata_disk_settings);
    };
    factory.registerDatabase("Ordinary", create_fn, /*features=*/{.supports_settings = true});
}
}
