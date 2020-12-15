#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageMerge.h>
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
    extern const int UNKNOWN_TABLE;
}


void TableFunctionMerge::parseArguments(const ASTPtr & ast_function, const Context & context)
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

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    args[1] = evaluateConstantExpressionAsLiteral(args[1], context);

    source_database = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    table_name_regexp = args[1]->as<ASTLiteral &>().value.safeGet<String>();
}


const Tables & TableFunctionMerge::getMatchingTables(const Context & context) const
{
    if (tables)
        return *tables;

    auto database = DatabaseCatalog::instance().getDatabase(source_database);

    OptimizedRegularExpression re(table_name_regexp);
    auto table_name_match = [&](const String & table_name_) { return re.match(table_name_); };

    auto access = context.getAccess();
    bool granted_show_on_all_tables = access->isGranted(AccessType::SHOW_TABLES, source_database);
    bool granted_select_on_all_tables = access->isGranted(AccessType::SELECT, source_database);

    tables.emplace();
    for (auto it = database->getTablesIterator(context, table_name_match); it->isValid(); it->next())
    {
        if (!it->table())
            continue;
        bool granted_show = granted_show_on_all_tables || access->isGranted(AccessType::SHOW_TABLES, source_database, it->name());
        if (!granted_show)
            continue;
        if (!granted_select_on_all_tables)
            access->checkAccess(AccessType::SELECT, source_database, it->name());
        tables->emplace(it->name(), it->table());
    }

    if (tables->empty())
        throw Exception("Error while executing table function merge. In database " + source_database + " no one matches regular expression: "
            + table_name_regexp, ErrorCodes::UNKNOWN_TABLE);

    return *tables;
}


ColumnsDescription TableFunctionMerge::getActualTableStructure(const Context & context) const
{
    auto first_table = getMatchingTables(context).begin()->second;
    return ColumnsDescription{first_table->getInMemoryMetadataPtr()->getColumns().getAllPhysical()};
}

StoragePtr TableFunctionMerge::executeImpl(const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto res = StorageMerge::create(
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context),
        source_database,
        getMatchingTables(context),
        context);

    res->startup();
    return res;
}


void registerTableFunctionMerge(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMerge>();
}

}
