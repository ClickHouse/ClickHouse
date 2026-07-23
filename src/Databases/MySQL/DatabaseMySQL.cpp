#include "config.h"

#if USE_MYSQL
#    include <filesystem>
#    include <string>
#    include <Columns/IColumn.h>
#    include <Core/Settings.h>
#    include <DataTypes/DataTypeDateTime.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/convertMySQLDataType.h>
#    include <Databases/DatabaseFactory.h>
#    include <Databases/MySQL/DatabaseMySQL.h>
#    include <Databases/MySQL/FetchTablesColumnsList.h>
#    include <Disks/IDisk.h>
#    include <IO/Operators.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/DatabaseCatalog.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/IAST_erase.h>
#    include <Parsers/ParserCreateQuery.h>
#    include <Parsers/parseQuery.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <Processors/Sources/MySQLSource.h>
#    include <QueryPipeline/QueryPipelineBuilder.h>
#    include <Storages/AlterCommands.h>
#    include <Storages/MySQL/MySQLHelpers.h>
#    include <Storages/MySQL/MySQLSettings.h>
#    include <Storages/NamedCollectionsHelpers.h>
#    include <Storages/StorageMySQL.h>
#    include <base/isSharedPtrUnique.h>
#    include <Common/escapeForFileName.h>
#    include <Common/filesystemHelpers.h>
#    include <Common/parseAddress.h>
#    include <Common/parseRemoteDescription.h>
#    include <Common/setThreadName.h>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace MySQLSetting
{
    extern const MySQLSettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int CANNOT_CREATE_DATABASE;
    extern const int BAD_ARGUMENTS;
}

constexpr static const auto suffix = ".remove_flag";
static constexpr const std::chrono::seconds cleaner_sleep_time{30};
static const std::chrono::seconds lock_acquire_timeout{10};

DatabaseMySQL::DatabaseMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    const ASTStorage * database_engine_define_,
    const String & database_name_in_mysql_,
    std::unique_ptr<MySQLSettings> settings_,
    mysqlxx::PoolWithFailover && pool,
    bool attach,
    UUID uuid)
    : IDatabase(database_name_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , database_name_in_mysql(database_name_in_mysql_)
    , mysql_settings(std::move(settings_))
    , mysql_pool(std::move(pool)) /// NOLINT
    , db_uuid(uuid)
{
    try
    {
        /// Test that the database is working fine; it will also fetch tables.
        empty(); // NOLINT(bugprone-standalone-empty)
    }
    catch (...)
    {
        if (attach)
            tryLogCurrentException("DatabaseMySQL");
        else
            throw;
    }

    if (persistent)
    {
        auto db_disk = getDisk();
        db_disk->createDirectories(metadata_path);
    }

    thread = ThreadFromGlobalPool{&DatabaseMySQL::cleanOutdatedTables, this};
}

bool DatabaseMySQL::empty() const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(getContext());

    if (local_tables_cache.empty())
        return true;

    for (const auto & [table_name, storage_info] : local_tables_cache)
        if (!remove_or_detach_tables.contains(table_name))
            return false;

    return true;
}

DatabaseTablesIteratorPtr DatabaseMySQL::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */) const
{
    Tables tables;
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(local_context);

    for (const auto & [table_name, modify_time_and_storage] : local_tables_cache)
        if (!remove_or_detach_tables.contains(table_name) && (!filter_by_table_name || filter_by_table_name(table_name)))
            tables[table_name] = modify_time_and_storage.second;

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}

bool DatabaseMySQL::isTableExist(const String & name, ContextPtr local_context) const
{
    return bool(tryGetTable(name, local_context));
}

StoragePtr DatabaseMySQL::tryGetTable(const String & mysql_table_name, ContextPtr local_context) const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(local_context);

    if (!remove_or_detach_tables.contains(mysql_table_name) && local_tables_cache.find(mysql_table_name) != local_tables_cache.end())
        return local_tables_cache[mysql_table_name].second;

    return StoragePtr{};
}

ASTPtr DatabaseMySQL::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(local_context);

    if (local_tables_cache.find(table_name) == local_tables_cache.end())
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "MySQL table {}.{} doesn't exist.", database_name_in_mysql, table_name);
        return nullptr;
    }

    auto storage = local_tables_cache[table_name].second;
    auto table_storage_define = database_engine_define->clone();
    {
        ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
        ast_storage->engine->kind = ASTFunction::Kind::TABLE_ENGINE;
        ASTs storage_children = ast_storage->children;
        auto storage_engine_arguments = ast_storage->engine->arguments;

        /// Add table_name to engine arguments
        if (typeid_cast<ASTIdentifier *>(storage_engine_arguments->children[0].get()))
        {
            storage_engine_arguments->children.push_back(
                makeASTFunction("equals", std::make_shared<ASTIdentifier>("table"), std::make_shared<ASTLiteral>(table_name)));
        }
        else
        {
            auto mysql_table_name = std::make_shared<ASTLiteral>(table_name);
            storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 2, mysql_table_name);
        }

        /// Unset settings
        std::erase_if(storage_children, [&](const ASTPtr & element) { return element.get() == ast_storage->settings; });
        ast_storage->settings = nullptr;
    }

    const Settings & settings = getContext()->getSettingsRef();
    auto create_table_query = DB::getCreateQueryFromStorage(
        storage,
        table_storage_define,
        true,
        static_cast<unsigned>(settings[Setting::max_parser_depth]),
        static_cast<unsigned>(settings[Setting::max_parser_backtracks]),
        throw_on_error);
    return create_table_query;
}

time_t DatabaseMySQL::getObjectMetadataModificationTime(const String & table_name) const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(getContext());

    if (local_tables_cache.find(table_name) == local_tables_cache.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "MySQL table {}.{} doesn't exist.", database_name_in_mysql, table_name);

    return time_t(local_tables_cache[table_name].first);
}

ASTPtr DatabaseMySQL::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_define);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}

void DatabaseMySQL::fetchTablesIntoLocalCache(ContextPtr local_context) const
{
    const auto & tables_with_modification_time = fetchTablesWithModificationTime(local_context);

    destroyLocalCacheExtraTables(tables_with_modification_time);
    fetchLatestTablesStructureIntoCache(tables_with_modification_time, local_context);
}

void DatabaseMySQL::destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const
{
    for (auto iterator = local_tables_cache.begin(); iterator != local_tables_cache.end();)
    {
        if (tables_with_modification_time.find(iterator->first) != tables_with_modification_time.end())
            ++iterator;
        else
        {
            outdated_tables.emplace_back(iterator->second.second);
            iterator = local_tables_cache.erase(iterator);
        }
    }
}

void DatabaseMySQL::fetchLatestTablesStructureIntoCache(
    const std::map<String, UInt64> & tables_modification_time, ContextPtr local_context) const
{
    std::vector<String> wait_update_tables_name;
    for (const auto & table_modification_time : tables_modification_time)
    {
        const auto & it = local_tables_cache.find(table_modification_time.first);

        /// Outdated or new table structures
        if (it == local_tables_cache.end() || table_modification_time.second > it->second.first)
            wait_update_tables_name.emplace_back(table_modification_time.first);
    }

    std::map<String, ColumnsDescription> tables_and_columns = fetchTablesColumnsList(wait_update_tables_name, local_context);

    for (const auto & table_and_columns : tables_and_columns)
    {
        const auto & table_name = table_and_columns.first;
        const auto & columns_name_and_type = table_and_columns.second;
        const auto & table_modification_time = tables_modification_time.at(table_name);

        const auto & iterator = local_tables_cache.find(table_name);
        if (iterator != local_tables_cache.end())
        {
            outdated_tables.emplace_back(iterator->second.second);
            local_tables_cache.erase(iterator);
        }

        local_tables_cache[table_name] = std::make_pair(
            table_modification_time,
            std::make_shared<StorageMySQL>(
                StorageID(database_name, table_name),
                std::move(mysql_pool),
                database_name_in_mysql,
                table_name,
                /* replace_query_ */ false,
                /* on_duplicate_clause = */ "",
                ColumnsDescription{columns_name_and_type},
                ConstraintsDescription{},
                String{},
                getContext(),
                MySQLSettings{}));
    }
}

std::map<String, UInt64> DatabaseMySQL::fetchTablesWithModificationTime(ContextPtr local_context) const
{
    Block tables_status_sample_block
    {
        { std::make_shared<DataTypeString>(),   "table_name" },
        { std::make_shared<DataTypeDateTime>(), "modification_time" },
    };

    WriteBufferFromOwnString query;
    query << "SELECT"
             " TABLE_NAME AS table_name, "
             " CREATE_TIME AS modification_time "
             " FROM INFORMATION_SCHEMA.TABLES "
             " WHERE TABLE_SCHEMA = " << quote << database_name_in_mysql;

    std::map<String, UInt64> tables_with_modification_time;
    StreamSettings mysql_input_stream_settings(local_context->getSettingsRef());
    auto result = std::make_unique<MySQLSource>(mysql_pool.get(), query.str(), tables_status_sample_block, mysql_input_stream_settings);
    QueryPipeline pipeline(std::move(result));

    Block block;
    PullingPipelineExecutor executor(pipeline);
    while (executor.pull(block))
    {
        size_t rows = block.rows();
        for (size_t index = 0; index < rows; ++index)
        {
            String table_name = (*block.getByPosition(0).column)[index].safeGet<String>();
            tables_with_modification_time[table_name] = (*block.getByPosition(1).column)[index].safeGet<UInt64>();
        }
    }

    return tables_with_modification_time;
}

std::map<String, ColumnsDescription>
DatabaseMySQL::fetchTablesColumnsList(const std::vector<String> & tables_name, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();

    return DB::fetchTablesColumnsList(
            mysql_pool,
            database_name_in_mysql,
            tables_name,
            settings,
            (*mysql_settings)[MySQLSetting::mysql_datatypes_support_level]);
}

void DatabaseMySQL::shutdown()
{
    std::map<String, ModifyTimeAndStorage> tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = local_tables_cache;
    }

    for (const auto & [table_name, modify_time_and_storage] : tables_snapshot)
        modify_time_and_storage.second->flushAndShutdown();

    std::lock_guard lock(mutex);
    local_tables_cache.clear();
}

void DatabaseMySQL::drop(ContextPtr)
{
    if (!persistent)
        return;

    auto db_disk = getDisk();
    db_disk->removeRecursive(getMetadataPath());
}

void DatabaseMySQL::cleanOutdatedTables()
{
    setThreadName("MySQLDBCleaner");

    std::unique_lock lock{mutex};

    while (!quit.load(std::memory_order_relaxed))
    {
        for (auto iterator = outdated_tables.begin(); iterator != outdated_tables.end();)
        {
            if (!isSharedPtrUnique(*iterator))
                ++iterator;
            else
            {
                const auto table_lock = (*iterator)->lockExclusively(RWLockImpl::NO_QUERY, lock_acquire_timeout);

                (*iterator)->flushAndShutdown();
                (*iterator)->is_dropped = true;
                iterator = outdated_tables.erase(iterator);
            }
        }

        cond.wait_for(lock, cleaner_sleep_time);
    }
}

void DatabaseMySQL::attachTable(ContextPtr /* context_ */, const String & table_name, const StoragePtr & storage, const String &)
{
    std::lock_guard lock{mutex};

    if (!local_tables_cache.contains(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot attach table {}.{} because it does not exist.",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    if (!remove_or_detach_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot attach table {}.{} because it already exists.",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    /// We use the new storage to replace the original storage, because the original storage may have been dropped
    /// Although we still keep its
    local_tables_cache[table_name].second = storage;

    remove_or_detach_tables.erase(table_name);
    fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);

    if (!persistent)
        return;

    auto db_disk = getDisk();
    db_disk->removeFileIfExists(remove_flag);
}

StoragePtr DatabaseMySQL::detachTable(ContextPtr /* context */, const String & table_name)
{
    std::lock_guard lock{mutex};

    if (remove_or_detach_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    if (!local_tables_cache.contains(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist.",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    remove_or_detach_tables.emplace(table_name);
    return local_tables_cache[table_name].second;
}

void DatabaseMySQL::alterDatabaseComment(const AlterCommand & command)
{
    DB::updateDatabaseCommentWithMetadataFile(shared_from_this(), command);
}

String DatabaseMySQL::getMetadataPath() const
{
    return metadata_path;
}

void DatabaseMySQL::loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel /*mode*/)
{
    if (!persistent)
        return;

    auto db_disk = getDisk();
    std::lock_guard lock{mutex};
    for (const auto it = db_disk->iterateDirectory(metadata_path); it->isValid(); it->next())
    {
        auto path = fs::path(it->path());
        if (path.filename().empty())
            path = path.parent_path();

        if (db_disk->existsFile(path) && endsWith(path.filename(), suffix))
        {
            const auto & filename = path.filename().filename().string();
            const auto & table_name = unescapeForFileName(filename.substr(0, filename.size() - strlen(suffix)));
            remove_or_detach_tables.emplace(table_name);
        }
    }
}

void DatabaseMySQL::detachTablePermanently(ContextPtr, const String & table_name)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DETACH TABLE is not supported for non-persistent MySQL database");

    auto db_disk = getDisk();
    std::lock_guard lock{mutex};

    fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);

    if (remove_or_detach_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    if (db_disk->existsFile(remove_flag))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The remove flag file already exists but the {}.{} does not exist remove tables, it is bug.",
                        backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    auto table_iter = local_tables_cache.find(table_name);
    if (table_iter == local_tables_cache.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    remove_or_detach_tables.emplace(table_name);

    try
    {
        table_iter->second.second->drop();
        db_disk->createFile(remove_flag);
    }
    catch (...)
    {
        remove_or_detach_tables.erase(table_name);
        throw;
    }
    table_iter->second.second->is_detached = true;
}

void DatabaseMySQL::dropTable(ContextPtr local_context, const String & table_name, bool /*sync*/)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP TABLE is not supported for non-persistent MySQL database");

    detachTablePermanently(local_context, table_name);
}

DatabaseMySQL::~DatabaseMySQL()
{
    try
    {
        if (!quit)
        {
            {
                quit = true;
                std::lock_guard lock{mutex};
            }
            cond.notify_one();
            thread.join();
        }

        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void DatabaseMySQL::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MySQL database engine does not support CREATE or ATTACH TABLE queries");

    const auto & create = create_query->as<ASTCreateQuery>();

    if (!create->attach)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "MySQL database engine does not support create table. "
                        "for tables that were detach or dropped before, you can use attach "
                        "to add them back to the MySQL database");

    /// XXX: hack
    /// In order to prevent users from broken the table structure by executing attach table database_name.table_name (...)
    /// we should compare the old and new create_query to make them completely consistent
    const auto & origin_create_query = getCreateTableQuery(table_name, getContext());
    origin_create_query->as<ASTCreateQuery>()->attach = true;

    if (origin_create_query->formatWithSecretsOneLine() != create_query->formatWithSecretsOneLine())
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                        "The MySQL database engine can only execute attach statements "
                        "of type attach table database_name.table_name");

    attachTable(local_context, table_name, storage, {});
}

void registerDatabaseMySQL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;
        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        StorageMySQL::Configuration configuration;
        ASTs & arguments = engine->arguments->children;
        auto mysql_settings = std::make_unique<MySQLSettings>();

        if (auto named_collection = tryGetNamedCollectionWithOverrides(arguments, args.context))
        {
            configuration = StorageMySQL::processNamedCollectionResult(*named_collection, *mysql_settings, args.context, false);
        }
        else
        {
            if (arguments.size() != 4)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "MySQL database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.");


            arguments[1] = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], args.context);
            const auto & host_port = safeGetLiteralValue<String>(arguments[0], engine_name);

            if (engine_name == "MySQL")
            {
                size_t max_addresses = args.context->getSettingsRef()[Setting::glob_expansion_max_elements];
                configuration.addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 3306);
            }
            else
            {
                const auto & [remote_host, remote_port] = parseAddress(host_port, 3306);
                configuration.host = remote_host;
                configuration.port = remote_port;
            }

            configuration.database = safeGetLiteralValue<String>(arguments[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(arguments[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(arguments[3], engine_name);
        }
        mysql_settings->loadFromQueryContext(args.context, *engine_define);
        if (engine_define->settings)
            mysql_settings->loadFromQuery(*engine_define);

        auto mysql_pool = createMySQLPoolWithFailover(configuration, *mysql_settings);

        try
        {
            return make_shared<DatabaseMySQL>(
                args.context,
                args.database_name,
                args.metadata_path,
                engine_define,
                configuration.database,
                std::move(mysql_settings),
                std::move(mysql_pool),
                args.create_query.attach,
                args.uuid);
        }
        catch (...)
        {
            const auto & exception_message = getCurrentExceptionMessage(true);
            throw Exception(ErrorCodes::CANNOT_CREATE_DATABASE, "Cannot create MySQL database, because {}", exception_message);
        }
    };
    factory.registerDatabase("MySQL", create_fn, {.supports_arguments = true, .supports_settings = true});
}
}

#endif
