#include <Databases/PostgreSQL/DatabasePostgreSQL.h>

#include <Parsers/ASTIdentifier.h>

#if USE_LIBPQXX

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StoragePostgreSQL.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>
#include <Common/parseRemoteDescription.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Common/quoteString.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

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
        bool cache_tables_)
    : IDatabase(dbname_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , configuration(configuration_)
    , pool(std::move(pool_))
    , cache_tables(cache_tables_)
    , log(getLogger("DatabasePostgreSQL(" + dbname_ + ")"))
{
    fs::create_directories(metadata_path);
    cleaner_task = getContext()->getSchedulePool().createTask("PostgreSQLCleanerTask", [this]{ removeOutdatedTables(); });
    cleaner_task->deactivate();
}


static String getTableNameForLogs(const String & database, const String & schema, const String & table_name)
{
    if (schema.empty())
        return fmt::format("{}.{}", database, table_name);
    return fmt::format("{}.{}.{}", database, schema, table_name);
}


String formatTableName(const String & schema, const String & table_name, bool quoted = true)
{
    if (schema.empty())
        return quoted ? doubleQuoteString(table_name) : table_name;
    return quoted ? fmt::format("{}.{}", doubleQuoteString(schema), doubleQuoteString(table_name))
                  : fmt::format("{}.{}", schema, table_name);
}


StoragePtr fetchTable(
    postgres::PoolWithFailoverPtr pool, pqxx::connection & connection,
    const String & database_name, const String & schema, const String & table_name,
    const String & on_conflict, ContextPtr context)
{
    auto columns_info = fetchPostgreSQLTableStructure(connection, table_name, schema).physical_columns;
    if (!columns_info)
        return StoragePtr{};

    return std::make_shared<StoragePostgreSQL>(
        StorageID(database_name, table_name), pool, table_name,
        ColumnsDescription{columns_info->columns}, ConstraintsDescription{}, String{}, context, schema, on_conflict);
}


bool DatabasePostgreSQL::empty() const
{
    String schema;
    std::unordered_set<std::string> local_detached_or_dropped;
    {
        std::lock_guard lock(mutex);
        schema = configuration.schema;
        local_detached_or_dropped = detached_or_dropped;
    }

    auto connection_holder = pool->get();
    auto tables_list = fetchPostgreSQLTablesList(connection_holder->get(), schema);

    for (const auto & table_name : tables_list)
        if (!local_detached_or_dropped.contains(table_name))
            return false;

    return true;
}


DatabaseTablesIteratorPtr DatabasePostgreSQL::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & /* filter_by_table_name */) const
{
    String schema;
    String local_database_name;
    String on_conflict;
    std::unordered_set<std::string> local_detached_or_dropped;
    bool local_cache_table;
    Tables tables;
    {
        std::lock_guard lock{mutex};
        schema = configuration.schema;
        local_database_name = database_name;
        on_conflict = configuration.on_conflict;
        local_detached_or_dropped = detached_or_dropped;
        local_cache_table = cache_tables;
        if (cache_tables)
            tables = cached_tables;
    }

    /// Do not allow to throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some postgres error.
    try
    {
        auto connection_holder = pool->get();
        auto & connection = connection_holder->get();
        auto table_names = fetchPostgreSQLTablesList(connection, schema);

        /// First remove outdated or dropped entries
        for (auto it = tables.cbegin();it != tables.cend();)
        {
            if (!table_names.contains(it->first) || !local_detached_or_dropped.contains(it->first))
                it = tables.erase(it);
            else
                ++it;
        }

        /// Then fetch all non-dropped tables we still need to add
        for (const auto & table_name : table_names)
            if (!local_detached_or_dropped.contains(table_name) && (!local_cache_table || !tables.count(table_name)))
                tables[table_name] = fetchTable(pool, connection, local_database_name, schema, table_name, on_conflict, local_context);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, local_database_name);
}


bool checkPostgresTable(pqxx::connection & connection, const String & schema, const String & table_name)
{
    if (table_name.find('\'') != std::string::npos
        || table_name.find('\\') != std::string::npos)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "PostgreSQL table name cannot contain single quote or backslash characters, passed {}", table_name);
    }

    pqxx::nontransaction tx(connection);

    try
    {
        /// Casting table_name::regclass throws pqxx::indefined_table exception if table_name is incorrect.
        pqxx::result result = tx.exec(fmt::format(
                    "SELECT '{}'::regclass, tablename "
                    "FROM pg_catalog.pg_tables "
                    "WHERE schemaname != 'pg_catalog' AND {} "
                    "AND tablename = '{}'",
                    formatTableName(schema, table_name),
                    (schema.empty() ? "schemaname != 'information_schema'" : "schemaname = " + quoteString(schema)),
                    formatTableName(schema, table_name)));
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
    String schema;
    {
        std::lock_guard lock(mutex);
        schema = configuration.schema;
        if (detached_or_dropped.contains(table_name))
            return false;
    }
    auto connection_holder = pool->get();
    return checkPostgresTable(connection_holder->get(), schema, table_name);
}


StoragePtr DatabasePostgreSQL::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    String schema;
    String local_database_name;
    String on_conflict;
    bool table_detached_or_dropped;
    StoragePtr storage{};
    {
        std::lock_guard lock{mutex};
        schema = configuration.schema;
        local_database_name = database_name;
        on_conflict = configuration.on_conflict;
        table_detached_or_dropped = detached_or_dropped.contains(table_name);
        if (!table_detached_or_dropped && cache_tables && cached_tables.contains(table_name))
            storage = cached_tables[table_name];
    }

    if (!table_detached_or_dropped)
    {
        auto connection_holder = pool->get();
        auto & connection = connection_holder->get();
        if (checkPostgresTable(connection, schema, table_name))
        {
            if (!storage)
                storage = fetchTable(pool, connection, local_database_name, schema, table_name, on_conflict, local_context);
        }
        else
        {
            storage = StoragePtr{};
        }
    }

    std::lock_guard lock{mutex};
    if (storage && cache_tables && !cached_tables.contains(table_name))
        cached_tables[table_name] = storage;
    if (!storage && cache_tables)
        cached_tables.erase(table_name);
    return storage;
}


void DatabasePostgreSQL::attachTable(ContextPtr /* context_ */, const String & table_name, const StoragePtr & storage, const String &)
{
    String database;
    String schema;
    String local_database_name;
    std::unordered_set<std::string> local_detached_or_dropped;
    {
        std::lock_guard lock{mutex};
        database = configuration.database;
        schema = configuration.schema;
        local_database_name = database_name;
        local_detached_or_dropped = detached_or_dropped;
    }
    {
        auto connection_holder = pool->get();
        auto & connection = connection_holder->get();
        if (!checkPostgresTable(connection, schema, table_name))
            throw Exception(
                ErrorCodes::UNKNOWN_TABLE,
                "Cannot attach PostgreSQL table {} because it does not exist in PostgreSQL (database: {})",
                getTableNameForLogs(database, schema, table_name),
                local_database_name);

        if (!local_detached_or_dropped.contains(table_name))
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS,
                "Cannot attach PostgreSQL table {} because it already exists (database: {})",
                getTableNameForLogs(database, schema, table_name),
                local_database_name);
    }

    std::lock_guard lock{mutex};
    if (cache_tables)
        cached_tables[table_name] = storage;

    detached_or_dropped.erase(table_name);

    fs::path table_marked_as_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
    if (fs::exists(table_marked_as_removed))
        fs::remove(table_marked_as_removed);
}


StoragePtr DatabasePostgreSQL::detachTable(ContextPtr /* context_ */, const String & table_name)
{
    String database;
    String schema;
    {
        std::lock_guard lock{mutex};
        database = configuration.database;
        schema = configuration.schema;
    }
    {
        auto connection_holder = pool->get();
        auto & connection = connection_holder->get();
        if (!checkPostgresTable(connection, schema, table_name))
            throw Exception(
                ErrorCodes::UNKNOWN_TABLE,
                "Cannot detach table {}, because it does not exist",
                getTableNameForLogs(database, schema, table_name));
    }

    std::lock_guard lock{mutex};

    if (detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Cannot detach table {}. It is already dropped/detached", getTableNameForLogs(database, schema, table_name));

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
    String database;
    String schema;
    {
        std::lock_guard lock{mutex};
        database = configuration.database;
        schema = configuration.schema;
    }
    {
        auto connection_holder = pool->get();
        auto & connection = connection_holder->get();
        if (!checkPostgresTable(connection, schema, table_name))
            throw Exception(
                ErrorCodes::UNKNOWN_TABLE,
                "Cannot drop table {} because it does not exist",
                getTableNameForLogs(database, schema, table_name));
    }

    std::lock_guard lock{mutex};

    if (detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is already dropped/detached", getTableNameForLogs(database, schema, table_name));

    fs::path mark_table_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
    FS::createFile(mark_table_removed);

    if (cache_tables)
        cached_tables.erase(table_name);

    detached_or_dropped.emplace(table_name);
}


void DatabasePostgreSQL::drop(ContextPtr /*context*/)
{
    fs::remove_all(getMetadataPath());
}


void DatabasePostgreSQL::loadStoredObjects(ContextMutablePtr /* context */, LoadingStrictnessLevel /*mode*/)
{
    {
        std::lock_guard lock{mutex};
        fs::directory_iterator iter(getMetadataPath());

        /// Check for previously dropped tables
        for (fs::directory_iterator end; iter != end; ++iter)
        {
            if (fs::is_regular_file(iter->path()) && endsWith(iter->path().filename(), suffix))
            {
                const auto & file_name = iter->path().filename().string();
                const auto & table_name = unescapeForFileName(file_name.substr(0, file_name.size() - strlen(suffix)));
                detached_or_dropped.emplace(table_name);
            }
        }
    }

    cleaner_task->activateAndSchedule();
}


void DatabasePostgreSQL::removeOutdatedTables()
{
    String schema;
    {
        std::lock_guard lock{mutex};
        // We schedule that immediately to make sure we schedule it even if anything fails
        cleaner_task->scheduleAfter(cleaner_reschedule_ms);
        // Early exit if we know we don't need to fetch the tables list at all
        // this avoids holding the mutex for too long if the remote PostgreSQL server is dead.
        if ((!cache_tables || cached_tables.empty()) && detached_or_dropped.empty())
            return;
        schema = configuration.schema;
    }
    std::lock_guard lock{mutex};

    std::set<std::string> actual_tables;
    try
    {
        auto connection_holder = pool->get();
        actual_tables = fetchPostgreSQLTablesList(connection_holder->get(), schema);
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
        cleaner_task->scheduleAfter(reschedule_error_multiplier * cleaner_reschedule_ms, /* overwrite= */ true);
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

    for (auto iter = detached_or_dropped.begin(); iter != detached_or_dropped.end();)
    {
        if (!actual_tables.contains(*iter))
        {
            auto table_name = *iter;
            iter = detached_or_dropped.erase(iter);
            fs::path table_marked_as_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
            if (fs::exists(table_marked_as_removed))
                fs::remove(table_marked_as_removed);
        }
        else
            ++iter;
    }
}


void DatabasePostgreSQL::shutdown()
{
    cleaner_task->deactivate();
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
    String database;
    String schema;
    String local_database_name;
    String on_conflict;
    bool table_detached_or_dropped;
    StoragePtr storage;
    {
        std::lock_guard lock{mutex};
        database = configuration.database;
        schema = configuration.schema;
        local_database_name = database_name;
        table_detached_or_dropped = detached_or_dropped.count(table_name);
        if (!table_detached_or_dropped && cache_tables && cached_tables.count(table_name))
            storage = cached_tables[table_name];
    }

    if (!table_detached_or_dropped)
    {
        auto connection_holder = pool->get();
        auto & connection = connection_holder->get();
        if (checkPostgresTable(connection, schema, table_name))
        {
            if (!storage)
                storage = fetchTable(pool, connection, local_database_name, schema, table_name, on_conflict, local_context);
        }
        else
        {
            storage = StoragePtr {};
        }
    }

    {
        std::lock_guard lock{mutex};
        if (storage && cache_tables && !cached_tables.count(table_name))
            cached_tables[table_name] = storage;
        else if (!storage && cache_tables)
            cached_tables.erase(table_name);
    }

    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "PostgreSQL table {} does not exist", getTableNameForLogs(database, schema, table_name));

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
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = getColumnDeclaration(column_type_and_name.type);
        columns_expression_list->children.emplace_back(column_declaration);
    }

    ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
    ASTs storage_children = ast_storage->children;
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
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTFunction("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    if (which.isDateTime64())
    {
        return makeASTFunction("DateTime64", std::make_shared<ASTLiteral>(static_cast<UInt32>(6)));
    }

    return std::make_shared<ASTIdentifier>(data_type->getName());
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
            size_t max_addresses = args.context->getSettingsRef().glob_expansion_max_elements;

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
        auto pool = postgres::PoolWithFailover::create(configuration, settings);

        return std::make_shared<DatabasePostgreSQL>(
            args.context,
            args.metadata_path,
            engine_define,
            args.database_name,
            configuration,
            pool,
            use_table_cache);
    };
    factory.registerDatabase("PostgreSQL", create_fn);
}
}

#endif
