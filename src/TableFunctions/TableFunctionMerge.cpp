#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageMerge.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <TableFunctions/TableFunctionMerge.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    [[noreturn]] void throwNoTablesMatchRegexp(const String & source_database_regexp, const String & source_table_regexp)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Error while executing table function merge. Either there is no database, which matches regular expression `{}`, or there are "
            "no tables in database matches `{}`, which fit tables expression: {}",
            source_database_regexp,
            source_database_regexp,
            source_table_regexp);
    }
}


void TableFunctionMerge::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function 'merge' requires exactly 2 arguments"
            " - name of source database and regexp for table names.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2)
        throw Exception("Table function 'merge' requires exactly 2 arguments"
            " - name of source database and regexp for table names.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto [is_regexp, database_ast] = StorageMerge::evaluateDatabaseName(args[0], context);

    database_is_regexp = is_regexp;

    if (!is_regexp)
        args[0] = database_ast;
    source_database_name_or_regexp = checkAndGetLiteralArgument<String>(database_ast, "database_name");

    args[1] = evaluateConstantExpressionAsLiteral(args[1], context);
    source_table_regexp = checkAndGetLiteralArgument<String>(args[1], "table_name_regexp");
}


const TableFunctionMerge::DBToTableSetMap & TableFunctionMerge::getSourceDatabasesAndTables(ContextPtr context) const
{
    if (source_databases_and_tables)
        return *source_databases_and_tables;

    source_databases_and_tables.emplace();

    /// database_name is not a regexp
    if (!database_is_regexp)
    {
        auto source_tables = getMatchedTablesWithAccess(source_database_name_or_regexp, source_table_regexp, context);
        if (source_tables.empty())
            throwNoTablesMatchRegexp(source_database_name_or_regexp, source_table_regexp);
        (*source_databases_and_tables)[source_database_name_or_regexp] = source_tables;
    }

    /// database_name is a regexp
    else
    {
        OptimizedRegularExpression database_re(source_database_name_or_regexp);
        auto databases = DatabaseCatalog::instance().getDatabases();

        for (const auto & db : databases)
            if (database_re.match(db.first))
                (*source_databases_and_tables)[db.first] = getMatchedTablesWithAccess(db.first, source_table_regexp, context);

        if (source_databases_and_tables->empty())
            throwNoTablesMatchRegexp(source_database_name_or_regexp, source_table_regexp);
    }

    return *source_databases_and_tables;
}

ColumnsDescription TableFunctionMerge::getActualTableStructure(ContextPtr context) const
{
    for (const auto & db_with_tables : getSourceDatabasesAndTables(context))
    {
        for (const auto & table : db_with_tables.second)
        {
            auto storage = DatabaseCatalog::instance().tryGetTable(StorageID{db_with_tables.first, table}, context);
            if (storage)
                return ColumnsDescription{storage->getInMemoryMetadataPtr()->getColumns().getAllPhysical()};
        }
    }

    throwNoTablesMatchRegexp(source_database_name_or_regexp, source_table_regexp);
}


StoragePtr TableFunctionMerge::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto res = std::make_shared<StorageMerge>(
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context),
        String{},
        source_database_name_or_regexp,
        database_is_regexp,
        getSourceDatabasesAndTables(context),
        context);

    res->startup();
    return res;
}

TableFunctionMerge::TableSet
TableFunctionMerge::getMatchedTablesWithAccess(const String & database_name, const String & table_regexp, const ContextPtr & context)
{
    OptimizedRegularExpression table_re(table_regexp);

    auto table_name_match = [&](const String & table_name) { return table_re.match(table_name); };

    auto access = context->getAccess();

    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    bool granted_show_on_all_tables = access->isGranted(AccessType::SHOW_TABLES, database_name);
    bool granted_select_on_all_tables = access->isGranted(AccessType::SELECT, database_name);

    TableSet tables;

    for (auto it = database->getTablesIterator(context, table_name_match); it->isValid(); it->next())
    {
        if (!it->table())
            continue;
        bool granted_show = granted_show_on_all_tables || access->isGranted(AccessType::SHOW_TABLES, database_name, it->name());
        if (!granted_show)
            continue;
        if (!granted_select_on_all_tables)
            access->checkAccess(AccessType::SELECT, database_name, it->name());
        tables.emplace(it->name());
    }
    return tables;
}

void registerTableFunctionMerge(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMerge>();
}

}
