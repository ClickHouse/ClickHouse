#include <Databases/PostgreSQL/DatabasePostgreSQL.h>

#include <Parsers/ASTIdentifier.h>

#if USE_LIBPQXX

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/StoragePostgreSQL.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Common/quoteString.h>
#include <Common/filesystemHelpers.h>
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

DatabasePostgreSQL::DatabasePostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        const ASTStorage * database_engine_define_,
        const String & dbname_,
        const StoragePostgreSQLConfiguration & configuration_,
        postgres::PoolWithFailoverPtr pool_,
        bool cache_tables_)
    : IDatabase(dbname_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , configuration(configuration_)
    , pool(std::move(pool_))
    , cache_tables(cache_tables_)
{
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
    std::lock_guard<std::mutex> lock(mutex);

    auto connection_holder = pool->get();
    auto tables_list = fetchPostgreSQLTablesList(connection_holder->get(), configuration.schema);

    for (const auto & table_name : tables_list)
        if (!detached_or_dropped.contains(table_name))
            return false;

    return true;
}


DatabaseTablesIteratorPtr DatabasePostgreSQL::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & /* filter_by_table_name */) const
{
    std::lock_guard<std::mutex> lock(mutex);
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
    if (table_name.find('\'') != std::string::npos
        || table_name.find('\\') != std::string::npos)
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
    std::lock_guard<std::mutex> lock(mutex);

    if (detached_or_dropped.contains(table_name))
        return false;

    return checkPostgresTable(table_name);
}


StoragePtr DatabasePostgreSQL::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (!detached_or_dropped.contains(table_name))
        return fetchTable(table_name, local_context, false);

    return StoragePtr{};
}


StoragePtr DatabasePostgreSQL::fetchTable(const String & table_name, ContextPtr, bool table_checked) const
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
                ColumnsDescription{columns_info->columns}, ConstraintsDescription{}, String{}, configuration.schema, configuration.on_conflict);

        if (cache_tables)
            cached_tables[table_name] = storage;

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
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
                        "Cannot attach PostgreSQL table {} because it does not exist in PostgreSQL",
                        getTableNameForLogs(table_name), database_name);

    if (!detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                        "Cannot attach PostgreSQL table {} because it already exists",
                        getTableNameForLogs(table_name), database_name);

    if (cache_tables)
        cached_tables[table_name] = storage;

    detached_or_dropped.erase(table_name);

    fs::path table_marked_as_removed = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
    if (fs::exists(table_marked_as_removed))
        fs::remove(table_marked_as_removed);
}


StoragePtr DatabasePostgreSQL::detachTable(ContextPtr /* context_ */, const String & table_name)
{
    std::lock_guard<std::mutex> lock{mutex};

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
        throw Exception("PostgreSQL database engine does not support create table", ErrorCodes::NOT_IMPLEMENTED);

    attachTable(local_context, table_name, storage, {});
}


void DatabasePostgreSQL::dropTable(ContextPtr, const String & table_name, bool /* no_delay */)
{
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot drop table {} because it does not exist", getTableNameForLogs(table_name));

    if (detached_or_dropped.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is already dropped/detached", getTableNameForLogs(table_name));

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


void DatabasePostgreSQL::loadStoredObjects(ContextMutablePtr /* context */, bool, bool /*force_attach*/, bool /* skip_startup_tables */)
{
    {
        std::lock_guard<std::mutex> lock{mutex};
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
    std::lock_guard<std::mutex> lock{mutex};
    auto connection_holder = pool->get();
    auto actual_tables = fetchPostgreSQLTablesList(connection_holder->get(), configuration.schema);

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

    cleaner_task->scheduleAfter(cleaner_reschedule_ms);
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
    auto storage = fetchTable(table_name, local_context, false);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "PostgreSQL table {} does not exist", getTableNameForLogs(table_name));

        return nullptr;
    }

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    auto table_storage_define = database_engine_define->clone();
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

    return std::make_shared<ASTIdentifier>(data_type->getName());
}

}

#endif
