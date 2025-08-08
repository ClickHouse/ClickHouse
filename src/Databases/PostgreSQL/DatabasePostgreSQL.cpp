#include <Databases/PostgreSQL/DatabasePostgreSQL.h>

#include <Parsers/ASTIdentifier.h>

#if USE_LIBPQXX

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/AlterCommands.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StoragePostgreSQL.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/escapeForFileName.h>
#include <Common/parseRemoteDescription.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Common/quoteString.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Core/BackgroundSchedulePool.h>
#include <filesystem>

#include <Disks/IDisk.h>
namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsUInt64 postgresql_connection_pool_size;
    extern const SettingsUInt64 postgresql_connection_pool_wait_timeout;
    extern const SettingsUInt64 postgresql_connection_pool_retries;
    extern const SettingsBool postgresql_connection_pool_auto_close_connection;
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
}

static const auto suffix = ".removed";
static const auto cleaner_reschedule_ms = 60000;
static const auto reschedule_error_multiplier = 10;

DatabasePostgreSQL::DatabasePostgreSQL(
    ContextPtr context_,
    const String & metadata_path_,
    const ASTStorage * database_engine_define_,
    const String & dbname_,
    const StoragePostgreSQL::Configuration & configuration_,
    postgres::PoolWithFailoverPtr pool_,
    bool cache_tables_,
    UUID uuid)
    : IDatabase(dbname_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , configuration(configuration_)
    , pool(std::move(pool_))
    , cache_tables(cache_tables_)
    , log(getLogger("DatabasePostgreSQL(" + dbname_ + ")"))
    , db_uuid(uuid)
{
    if (persistent)
    {
        auto db_disk = getDisk();
        db_disk->createDirectories(metadata_path);
    }

    cleaner_task = getContext()->getSchedulePool().createTask("PostgreSQLCleanerTask", [this]{ removeOutdatedTables(); });
    cleaner_task->deactivate();
}


String DatabasePostgreSQL::getTableNameForLogs(const String & table_name) const
{
    if (configuration.schema.empty())
        return fmt::format("{}.{}", configuration.database, table_name);
    return fmt::format("{}.{}.{}", configuration.database, configuration.schema, table_name);
}


String DatabasePostgreSQL::formatTableName(const String & table_name, bool quoted) const
{
    if (configuration.schema.empty())
        return quoted ? doubleQuoteString(table_name) : table_name;
    return quoted ? fmt::format("{}.{}", doubleQuoteString(configuration.schema), doubleQuoteString(table_name))
                  : fmt::format("{}.{}", configuration.schema, table_name);
}


bool DatabasePostgreSQL::empty() const
{
    std::lock_guard lock(mutex);

    auto connection_holder = pool->get();
    auto tables_list = fetchPostgreSQLTablesList(connection_holder->get(), configuration.schema);

    for (const auto & table_name : tables_list)
        if (!detached_or_dropped.contains(table_name))
            return false;

    return true;
}


DatabaseTablesIteratorPtr DatabasePostgreSQL::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & /* filter_by_table_name */, bool /* skip_not_loaded */) const
{
    std::lock_guard lock(mutex);
    Tables tables;

    /// Do not allow to throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some postgres error.
    try
    {
        auto connection_holder = pool->get();
        auto table_names = fetchPostgreSQLTablesList(connection_holder->get(), configuration.schema);

        for (const auto & table_name : table_names)
            if (!detached_or_dropped.contains(table_name))
                tables[table_name] = fetchTable(table_name, local_context, true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}


bool DatabasePostgreSQL::checkPostgresTable(const String & table_name) const
{
    if (table_name.contains('\'') || table_name.contains('\\'))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "PostgreSQL table name cannot contain single quote or backslash characters, passed {}", table_name);
    }

    auto connection_holder = pool->get();
    pqxx::nontransaction tx(connection_holder->get());

    try
    {
        /// Casting table_name::regclass throws pqxx::indefined_table exception if table_name is incorrect.
        pqxx::result result = tx.exec(fmt::format(
                    "SELECT '{}'::regclass, tablename "
                    "FROM pg_catalog.pg_tables "
                    "WHERE schemaname != 'pg_catalog' AND {} "
                    "AND tablename = '{}'",
                    formatTableName(table_name),
                    (configuration.schema.empty() ? "schemaname != 'information_schema'" : "schemaname = " + quoteString(configuration.schema)),
                    formatTableName(table_name)));
    }
    catch (pqxx::undefined_table const &)
    {
        return false;
    }
    catch (Exception & e)
    {
        e.addMessage("while checking postgresql table existence");
        throw;
    }

    return true;
}


bool DatabasePostgreSQL::isTableExist(const String & table_name, ContextPtr /* context */) const
{
    std::lock_guard lock(mutex);

    if (detached_or_dropped.contains(table_name))
        return false;

    return checkPostgresTable(table_name);
}


StoragePtr DatabasePostgreSQL::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    std::lock_guard lock(mutex);

    if (!detached_or_dropped.contains(table_name))
        return fetchTable(table_name, local_context, false);

    return StoragePtr{};
}


StoragePtr DatabasePostgreSQL::fetchTable(const String & table_name, ContextPtr context_, bool table_checked) const
{
    if (!cache_tables || !cached_tables.contains(table_name))
    {
        if (!table_checked && !checkPostgresTable(table_name))
            return StoragePtr{};

        auto connection_holder = pool->get();
        auto columns_info = fetchPostgreSQLTableStructure(connection_holder->get(), table_name, configuration.schema).physical_columns;

        if (!columns_info)
            return StoragePtr{};

        auto storage = std::make_shared<StoragePostgreSQL>(
                StorageID(database_name, table_name), pool, table_name,
                ColumnsDescription{columns_info->columns}, ConstraintsDescription{}, String{},
                context_, configuration.schema, configuration.on_conflict);

        if (cache_tables)
        {
            LOG_TEST(log, "Cached table `{}`", table_name);
            cached_tables[table_name] = storage;
        }

        return storage;
    }

    if (table_checked || checkPostgresTable(table_name))
    {
        return cached_tables[table_name];
    }

    /// Table does not exist anymore
    cached_tables.erase(table_name);
    return StoragePtr{};
}


void DatabasePostgreSQL::attachTable(ContextPtr /* context_ */, const String & table_name, const StoragePtr & storage, const String &)
{
    auto db_disk = getDisk();
    std::lock_guard lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
                        "Cannot attach PostgreSQL table {} because it does not exist in PostgreSQL (database: {})",
                        getTableNameForLogs(table_name), database_name);

    if (!detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                        "Cannot attach PostgreSQL table {} because it already exists (database: {})",
                        getTableNameForLogs(table_name), database_name);

    if (cache_tables)
        cached_tables[table_name] = storage;

    detached_or_dropped.erase(table_name);

    if (!persistent)
        return;

    fs::path table_marked_as_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
    db_disk->removeFileIfExists(table_marked_as_removed);
}


StoragePtr DatabasePostgreSQL::detachTable(ContextPtr /* context_ */, const String & table_name)
{
    std::lock_guard lock{mutex};

    if (detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Cannot detach table {}. It is already dropped/detached", getTableNameForLogs(table_name));

    if (!checkPostgresTable(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot detach table {}, because it does not exist", getTableNameForLogs(table_name));

    if (cache_tables)
        cached_tables.erase(table_name);

    detached_or_dropped.emplace(table_name);

    /// not used anywhere (for postgres database)
    return StoragePtr{};
}


void DatabasePostgreSQL::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query)
{
    const auto & create = create_query->as<ASTCreateQuery>();

    if (!create->attach)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PostgreSQL database engine does not support create table");

    attachTable(local_context, table_name, storage, {});
}


void DatabasePostgreSQL::dropTable(ContextPtr, const String & table_name, bool /* sync */)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP TABLE is not supported for non-persistent MySQL database");

    auto db_disk = getDisk();
    std::lock_guard lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot drop table {} because it does not exist", getTableNameForLogs(table_name));

    if (detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is already dropped/detached", getTableNameForLogs(table_name));

    fs::path mark_table_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
    db_disk->createFile(mark_table_removed);

    if (cache_tables)
        cached_tables.erase(table_name);

    detached_or_dropped.emplace(table_name);
}


void DatabasePostgreSQL::drop(ContextPtr)
{
    if (!persistent)
        return;

    auto db_disk = getDisk();
    db_disk->removeRecursive(getMetadataPath());
}


void DatabasePostgreSQL::loadStoredObjects(ContextMutablePtr /* context */, LoadingStrictnessLevel /*mode*/)
{
    if (persistent)
    {
        auto db_disk = getDisk();
        std::lock_guard lock{mutex};
        /// Check for previously dropped tables
        for (const auto it = db_disk->iterateDirectory(getMetadataPath()); it->isValid(); it->next())
        {
            auto path = fs::path(it->path());
            if (path.filename().empty())
                path = path.parent_path();

            if (db_disk->existsFile(path) && endsWith(path.filename(), suffix))
            {
                const auto & file_name = path.filename().string();
                const auto & table_name = unescapeForFileName(file_name.substr(0, file_name.size() - strlen(suffix)));
                detached_or_dropped.emplace(table_name);
            }
        }
    }

    cleaner_task->activateAndSchedule();
}


void DatabasePostgreSQL::removeOutdatedTables()
{
    std::lock_guard lock{mutex};

    std::set<std::string> actual_tables;
    try
    {
        auto connection_holder = pool->get();
        actual_tables = fetchPostgreSQLTablesList(connection_holder->get(), configuration.schema);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /** Avoid repeated interrupting other normal routines (they acquire locks!)
          * for the case of unavailable connection, since it is possible to be
          * unsuccessful again, and the unsuccessful conn is very time-consuming:
          * connection period is exclusive and timeout is at least 2 seconds for
          * PostgreSQL.
          */
        cleaner_task->scheduleAfter(reschedule_error_multiplier * cleaner_reschedule_ms);
        return;
    }

    if (cache_tables)
    {
        /// (Tables are cached only after being accessed at least once)
        for (auto iter = cached_tables.begin(); iter != cached_tables.end();)
        {
            if (!actual_tables.contains(iter->first))
                iter = cached_tables.erase(iter);
            else
                ++iter;
        }
    }

    auto db_disk = getDisk();
    for (auto iter = detached_or_dropped.begin(); iter != detached_or_dropped.end();)
    {
        if (!actual_tables.contains(*iter))
        {
            auto table_name = *iter;
            iter = detached_or_dropped.erase(iter);

            if (!persistent)
                continue;

            fs::path table_marked_as_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
            db_disk->removeFileIfExists(table_marked_as_removed);
        }
        else
            ++iter;
    }

    cleaner_task->scheduleAfter(cleaner_reschedule_ms);
}


void DatabasePostgreSQL::shutdown()
{
    cleaner_task->deactivate();
}

void DatabasePostgreSQL::alterDatabaseComment(const AlterCommand & command)
{
    DB::updateDatabaseCommentWithMetadataFile(shared_from_this(), command);
}

ASTPtr DatabasePostgreSQL::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_define);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}


ASTPtr DatabasePostgreSQL::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage;
    {
        std::lock_guard lock{mutex};
        storage = fetchTable(table_name, local_context, false);
    }
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "PostgreSQL table {} does not exist", getTableNameForLogs(table_name));

        return nullptr;
    }

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    auto table_storage_define = database_engine_define->clone();
    table_storage_define->as<ASTStorage>()->engine->kind = ASTFunction::Kind::TABLE_ENGINE;
    create_table_query->set(create_table_query->storage, table_storage_define);

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    /// init create query.
    auto table_id = storage->getStorageID();
    create_table_query->setTable(table_id.table_name);
    create_table_query->setDatabase(table_id.database_name);

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
    for (const auto & column_type_and_name : metadata_snapshot->getColumns().getOrdinary())
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = getColumnDeclaration(column_type_and_name.type);
        columns_expression_list->children.emplace_back(column_declaration);
    }

    ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
    auto storage_engine_arguments = ast_storage->engine->arguments;

    if (storage_engine_arguments->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected number of arguments: {}", storage_engine_arguments->children.size());

    /// Check for named collection.
    if (typeid_cast<ASTIdentifier *>(storage_engine_arguments->children[0].get()))
    {
        storage_engine_arguments->children.push_back(makeASTFunction("equals", std::make_shared<ASTIdentifier>("table"), std::make_shared<ASTLiteral>(table_id.table_name)));
    }
    else
    {
        /// Remove extra engine argument (`schema` and `use_table_cache`)
        if (storage_engine_arguments->children.size() >= 5)
            storage_engine_arguments->children.resize(4);

        /// Add table_name to engine arguments.
        if (storage_engine_arguments->children.size() >= 2)
            storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 2, std::make_shared<ASTLiteral>(table_id.table_name));
    }

    return create_table_query;
}


ASTPtr DatabasePostgreSQL::getColumnDeclaration(const DataTypePtr & data_type) const
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTDataType("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTDataType("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    if (which.isDateTime64())
        return makeASTDataType("DateTime64", std::make_shared<ASTLiteral>(static_cast<UInt32>(6)));

    return makeASTDataType(data_type->getName());
}

void registerDatabasePostgreSQL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        ASTs & engine_args = engine->arguments->children;
        const String & engine_name = engine_define->engine->name;

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        auto use_table_cache = false;
        StoragePostgreSQL::Configuration configuration;

        if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, args.context))
        {
            configuration = StoragePostgreSQL::processNamedCollectionResult(*named_collection, args.context, false);
            use_table_cache = named_collection->getOrDefault<UInt64>("use_table_cache", 0);
        }
        else
        {
            if (engine_args.size() < 4 || engine_args.size() > 6)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "PostgreSQL Database require `host:port`, `database_name`, `username`, `password`"
                                "[, `schema` = "", `use_table_cache` = 0");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

            const auto & host_port = safeGetLiteralValue<String>(engine_args[0], engine_name);
            size_t max_addresses = args.context->getSettingsRef()[Setting::glob_expansion_max_elements];

            configuration.addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 5432);
            configuration.database = safeGetLiteralValue<String>(engine_args[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(engine_args[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(engine_args[3], engine_name);

            bool is_deprecated_syntax = false;
            if (engine_args.size() >= 5)
            {
                auto arg_value = engine_args[4]->as<ASTLiteral>()->value;
                if (arg_value.getType() == Field::Types::Which::String)
                {
                    configuration.schema = safeGetLiteralValue<String>(engine_args[4], engine_name);
                }
                else
                {
                    use_table_cache = safeGetLiteralValue<UInt8>(engine_args[4], engine_name);
                    LOG_WARNING(getLogger("DatabaseFactory"), "A deprecated syntax of PostgreSQL database engine is used");
                    is_deprecated_syntax = true;
                }
            }

            if (!is_deprecated_syntax && engine_args.size() >= 6)
                use_table_cache = safeGetLiteralValue<UInt8>(engine_args[5], engine_name);
        }

        const auto & settings = args.context->getSettingsRef();
        auto pool = std::make_shared<postgres::PoolWithFailover>(
            configuration,
            settings[Setting::postgresql_connection_pool_size],
            settings[Setting::postgresql_connection_pool_wait_timeout],
            settings[Setting::postgresql_connection_pool_retries],
            settings[Setting::postgresql_connection_pool_auto_close_connection],
            settings[Setting::postgresql_connection_attempt_timeout]);

        return std::make_shared<DatabasePostgreSQL>(
            args.context,
            args.metadata_path,
            engine_define,
            args.database_name,
            configuration,
            pool,
            use_table_cache,
            args.uuid);
    };
    factory.registerDatabase("PostgreSQL", create_fn, {.supports_arguments = true});
}
}

#endif
