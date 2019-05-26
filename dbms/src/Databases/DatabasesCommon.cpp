#include <Databases/DatabasesCommon.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Common/typeid_cast.h>

#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int INCORRECT_QUERY;
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


String getDictionaryDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_clone.get());
    if (create.dictionary.empty())
        throw Exception("Can't get dictionary definition from create query", ErrorCodes::BAD_ARGUMENTS);

    // TODO: maybe add more accurate check of ASTCreateQuery

    std::ostringstream statement_stream;
    formatAST(create, statement_stream, false);
    statement_stream << "\n";
    return statement_stream.str();
}


std::pair<String, DictionaryPtr> createDictionaryFromDefinition(
    const String & definition,
    const String & database_name,
    Context & context,
    const String & description_for_error_message)
{
    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(), description_for_error_message, 0);
    if (!ast || dynamic_cast<ASTCreateQuery*>(ast.get()) == nullptr)
        throw Exception("Can't parse ASTCreateQuery", ErrorCodes::INCORRECT_QUERY);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.database = database_name;

    if (!ast_create_query.columns_list)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    auto dictionary_ptr = DictionaryFactory::instance().create(ast_create_query.dictionary, ast_create_query, context);

    return {
        ast_create_query.dictionary,
        dictionary_ptr,
    };
}

bool DatabaseWithOwnTablesBase::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(tables_mutex);
    return tables.find(table_name) != tables.end();
}


bool DatabaseWithOwnTablesBase::isDictionaryExist(const Context & context, const String &) const
{
    throw Exception(context.getDatabase(name)->getEngineName() + ": isDictionaryExist() isn't supported.", ErrorCodes::NOT_IMPLEMENTED);
}


StoragePtr DatabaseWithOwnTablesBase::tryGetTable(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(tables_mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        return {};
    return it->second;
}


DictionaryPtr DatabaseWithOwnTablesBase::tryGetDictionary(const Context & context, const String &) const
{
    throw Exception(context.getDatabase(name)->getEngineName() + ": tryGetDictionary() isn't supported.", ErrorCodes::NOT_IMPLEMENTED);
}


DictionaryPtr DatabaseWithOwnTablesBase::getDictionary(
    const Context & context,
    const String &) const
{
    throw Exception(context.getDatabase(name)->getEngineName() + ": getDictionary() isn't supported.", ErrorCodes::NOT_IMPLEMENTED);
}


void DatabaseWithOwnTablesBase::loadDictionaries(Context &, ThreadPool *)
{
}


void DatabaseWithOwnTablesBase::createDictionary(Context & context, const String &, const DictionaryPtr &, const ASTPtr &)
{
    throw Exception(context.getDatabase(name)->getEngineName() + ": createDicitonary() isn't supported.", ErrorCodes::NOT_IMPLEMENTED);
}


void DatabaseWithOwnTablesBase::removeDictionary(Context & context, const String & /*dictionary_name*/)
{
    throw Exception(context.getDatabase(name)->getEngineName() + ": removeDictionary() isn't supported.", ErrorCodes::NOT_IMPLEMENTED);
}

DatabaseIteratorPtr DatabaseWithOwnTablesBase::getIterator(const Context & /*context*/)
{
    std::lock_guard lock(tables_mutex);
    return std::make_unique<DatabaseSnapshotIterator>(tables);
}

bool DatabaseWithOwnTablesBase::empty(const Context & /*context*/) const
{
    std::lock_guard lock(tables_mutex);
    return tables.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        std::lock_guard lock(tables_mutex);
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
    std::lock_guard lock(tables_mutex);
    if (!tables.emplace(table_name, table).second)
        throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(tables_mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard lock(tables_mutex);
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
