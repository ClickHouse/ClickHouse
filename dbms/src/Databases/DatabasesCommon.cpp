#include <sstream>

#include <Common/typeid_cast.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Storages/StorageFactory.h>
#include <Databases/DatabasesCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}


String getTableDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    auto & create = query_clone->as<ASTCreateQuery &>();

    /// We remove everything that is not needed for ATTACH from the query.
    create.attach = true;
    create.database.clear();
    create.as_database.clear();
    create.as_table.clear();
    create.if_not_exists = false;
    create.is_populate = false;
    create.replace_view = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create.is_view && !create.is_materialized_view)
        create.select = nullptr;

    create.format = nullptr;
    create.out_file = nullptr;

    std::ostringstream statement_stream;
    formatAST(create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
}


std::pair<String, StoragePtr> createTableFromDefinition(
    const String & definition,
    const String & database_name,
    const String & database_data_path,
    Context & context,
    bool has_force_restore_data_flag,
    const String & description_for_error_message)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(), description_for_error_message, 0);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.attach = true;
    ast_create_query.database = database_name;

    /// We do not directly use `InterpreterCreateQuery::execute`, because
    /// - the database has not been created yet;
    /// - the code is simpler, since the query is already brought to a suitable form.
    if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context);

    return
    {
        ast_create_query.table,
        StorageFactory::instance().get(
            ast_create_query,
            database_data_path, ast_create_query.table, database_name, context, context.getGlobalContext(),
            columns,
            true, has_force_restore_data_flag)
    };
}


bool DatabaseWithOwnTablesBase::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        return {};
    return it->second;
}

DatabaseIteratorPtr DatabaseWithOwnTablesBase::getIterator(const Context & /*context*/)
{
    std::lock_guard lock(mutex);
    return std::make_unique<DatabaseSnapshotIterator>(tables);
}

bool DatabaseWithOwnTablesBase::empty(const Context & /*context*/) const
{
    std::lock_guard lock(mutex);
    return tables.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        std::lock_guard lock(mutex);
        auto it = tables.find(table_name);
        if (it == tables.end())
            throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        res = it->second;
        tables.erase(it);
    }

    return res;
}

void DatabaseWithOwnTablesBase::attachTable(const String & table_name, const StoragePtr & table)
{
    std::lock_guard lock(mutex);
    if (!tables.emplace(table_name, table).second)
        throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard lock(mutex);
    tables.clear();
}

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
