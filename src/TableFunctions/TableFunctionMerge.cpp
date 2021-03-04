#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageMerge.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
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


static NamesAndTypesList chooseColumns(const String & source_database, const String & table_name_regexp_, const Context & context)
{
    OptimizedRegularExpression table_name_regexp(table_name_regexp_);
    auto table_name_match = [&](const String & table_name) { return table_name_regexp.match(table_name); };

    StoragePtr any_table;

    {
        auto database = DatabaseCatalog::instance().getDatabase(source_database);
        auto iterator = database->getTablesIterator(context, table_name_match);

        if (iterator->isValid())
            if (const auto & table = iterator->table())
                any_table = table;
    }

    if (!any_table)
        throw Exception("Error while executing table function merge. In database " + source_database + " no one matches regular expression: "
            + table_name_regexp_, ErrorCodes::UNKNOWN_TABLE);

    return any_table->getInMemoryMetadataPtr()->getColumns().getAllPhysical();
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

ColumnsDescription TableFunctionMerge::getActualTableStructure(const Context & context) const
{
    return ColumnsDescription{chooseColumns(source_database, table_name_regexp, context)};
}

StoragePtr TableFunctionMerge::executeImpl(const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto res = StorageMerge::create(
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context),
        source_database,
        table_name_regexp,
        context);

    res->startup();
    return res;
}


void registerTableFunctionMerge(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMerge>();
}

}
