#include <Databases/PostgreSQL/DatabasePostgreSQL.h>

#if USE_LIBPQXX

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/PostgreSQL/PostgreSQLConnection.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>


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
        const Context & context,
        const String & metadata_path_,
        const ASTStorage * database_engine_define_,
        const String & dbname_,
        const String & postgres_dbname,
        PostgreSQLConnectionPtr connection_,
        const bool cache_tables_)
    : IDatabase(dbname_)
    , global_context(context.getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , dbname(postgres_dbname)
    , connection(std::move(connection_))
    , cache_tables(cache_tables_)
{
    cleaner_task = context.getSchedulePool().createTask("PostgreSQLCleanerTask", [this]{ removeOutdatedTables(); });
    cleaner_task->deactivate();
}


bool DatabasePostgreSQL::empty() const
{
    std::lock_guard<std::mutex> lock(mutex);

    auto tables_list = fetchPostgreSQLTablesList(connection->conn());

    for (const auto & table_name : tables_list)
        if (!detached_or_dropped.count(table_name))
            return false;

    return true;
}


DatabaseTablesIteratorPtr DatabasePostgreSQL::getTablesIterator(
        const Context & context, const FilterByNameFunction & /* filter_by_table_name */)
{
    std::lock_guard<std::mutex> lock(mutex);

    Tables tables;
    auto table_names = fetchPostgreSQLTablesList(connection->conn());

    for (const auto & table_name : table_names)
        if (!detached_or_dropped.count(table_name))
            tables[table_name] = fetchTable(table_name, context, true);

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

    pqxx::nontransaction tx(*connection->conn());

    try
    {
        /// Casting table_name::regclass throws pqxx::indefined_table exception if table_name is incorrect.
        pqxx::result result = tx.exec(fmt::format(
                    "SELECT '{}'::regclass, tablename "
                    "FROM pg_catalog.pg_tables "
                    "WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' "
                    "AND tablename = '{}'", table_name, table_name));
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


bool DatabasePostgreSQL::isTableExist(const String & table_name, const Context & /* context */) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (detached_or_dropped.count(table_name))
        return false;

    return checkPostgresTable(table_name);
}


StoragePtr DatabasePostgreSQL::tryGetTable(const String & table_name, const Context & context) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (!detached_or_dropped.count(table_name))
        return fetchTable(table_name, context, false);

    return StoragePtr{};
}


StoragePtr DatabasePostgreSQL::fetchTable(const String & table_name, const Context & context, const bool table_checked) const
{
    if (!cache_tables || !cached_tables.count(table_name))
    {
        if (!table_checked && !checkPostgresTable(table_name))
            return StoragePtr{};

        auto use_nulls = context.getSettingsRef().external_table_functions_use_nulls;
        auto columns = fetchPostgreSQLTableStructure(connection->conn(), table_name, use_nulls);

        if (!columns)
            return StoragePtr{};

        auto storage = StoragePostgreSQL::create(
                StorageID(database_name, table_name), table_name, std::make_shared<PostgreSQLConnection>(connection->conn_str()),
                ColumnsDescription{*columns}, ConstraintsDescription{}, context);

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


void DatabasePostgreSQL::attachTable(const String & table_name, const StoragePtr & storage, const String &)
{
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(fmt::format("Cannot attach table {}.{} because it does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

    if (!detached_or_dropped.count(table_name))
        throw Exception(fmt::format("Cannot attach table {}.{}. It already exists", database_name, table_name), ErrorCodes::TABLE_ALREADY_EXISTS);

    if (cache_tables)
        cached_tables[table_name] = storage;

    detached_or_dropped.erase(table_name);

    Poco::File table_marked_as_removed(getMetadataPath() + '/' + escapeForFileName(table_name) + suffix);
    if (table_marked_as_removed.exists())
        table_marked_as_removed.remove();
}


StoragePtr DatabasePostgreSQL::detachTable(const String & table_name)
{
    std::lock_guard<std::mutex> lock{mutex};

    if (detached_or_dropped.count(table_name))
        throw Exception(fmt::format("Cannot detach table {}.{}. It is already dropped/detached", database_name, table_name), ErrorCodes::TABLE_IS_DROPPED);

    if (!checkPostgresTable(table_name))
        throw Exception(fmt::format("Cannot detach table {}.{} because it does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

    if (cache_tables)
        cached_tables.erase(table_name);

    detached_or_dropped.emplace(table_name);

    /// not used anywhere (for postgres database)
    return StoragePtr{};
}


void DatabasePostgreSQL::createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query)
{
    const auto & create = create_query->as<ASTCreateQuery>();

    if (!create->attach)
        throw Exception("PostgreSQL database engine does not support create table", ErrorCodes::NOT_IMPLEMENTED);

    attachTable(table_name, storage, {});
}


void DatabasePostgreSQL::dropTable(const Context &, const String & table_name, bool /* no_delay */)
{
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(fmt::format("Cannot drop table {}.{} because it does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

    if (detached_or_dropped.count(table_name))
        throw Exception(fmt::format("Table {}.{} is already dropped/detached", database_name, table_name), ErrorCodes::TABLE_IS_DROPPED);

    Poco::File mark_table_removed(getMetadataPath() + '/' + escapeForFileName(table_name) + suffix);

    try
    {
        mark_table_removed.createFile();
    }
    catch (...)
    {
        throw;
    }

    if (cache_tables)
        cached_tables.erase(table_name);

    detached_or_dropped.emplace(table_name);
}


void DatabasePostgreSQL::drop(const Context & /*context*/)
{
    Poco::File(getMetadataPath()).remove(true);
}


void DatabasePostgreSQL::loadStoredObjects(Context & /* context */, bool, bool /*force_attach*/)
{
    {
        std::lock_guard<std::mutex> lock{mutex};
        Poco::DirectoryIterator iterator(getMetadataPath());

        /// Check for previously dropped tables
        for (Poco::DirectoryIterator end; iterator != end; ++iterator)
        {
            if (iterator->isFile() && endsWith(iterator.name(), suffix))
            {
                const auto & file_name = iterator.name();
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
    auto actual_tables = fetchPostgreSQLTablesList(connection->conn());

    if (cache_tables)
    {
        /// (Tables are cached only after being accessed at least once)
        for (auto iter = cached_tables.begin(); iter != cached_tables.end();)
        {
            if (!actual_tables.count(iter->first))
                iter = cached_tables.erase(iter);
            else
                ++iter;
        }
    }

    for (auto iter = detached_or_dropped.begin(); iter != detached_or_dropped.end();)
    {
        if (!actual_tables.count(*iter))
        {
            auto table_name = *iter;
            iter = detached_or_dropped.erase(iter);
            Poco::File table_marked_as_removed(getMetadataPath() + '/' + escapeForFileName(table_name) + suffix);
            if (table_marked_as_removed.exists())
                table_marked_as_removed.remove();
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
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, database_engine_define);
    return create_query;
}


ASTPtr DatabasePostgreSQL::getCreateTableQueryImpl(const String & table_name, const Context & context, bool throw_on_error) const
{
    auto storage = fetchTable(table_name, context, false);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(fmt::format("PostgreSQL table {}.{} does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

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
    create_table_query->table = table_id.table_name;
    create_table_query->database = table_id.database_name;

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

    /// Remove extra engine argument (`use_table_cache`)
    if (storage_engine_arguments->children.size() > 4)
        storage_engine_arguments->children.resize(storage_engine_arguments->children.size() - 1);

    /// Add table_name to engine arguments
    assert(storage_engine_arguments->children.size() >= 2);
    storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 2, std::make_shared<ASTLiteral>(table_id.table_name));

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
