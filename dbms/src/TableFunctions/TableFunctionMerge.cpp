#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageMerge.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <TableFunctions/TableFunctionMerge.h>
#include <TableFunctions/TableFunctionFactory.h>


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

    StoragePtr any_table;

    {
        auto database = context.getDatabase(source_database);
        auto iterator = database->getIterator(context);

        while (iterator->isValid())
        {
            if (table_name_regexp.match(iterator->name()))
            {
                any_table = iterator->table();
                break;
            }

            iterator->next();
        }
    }

    if (!any_table)
        throw Exception("Error while executing table function merge. In database " + source_database + " no one matches regular expression: "
            + table_name_regexp_, ErrorCodes::UNKNOWN_TABLE);

    return any_table->getColumns().getAllPhysical();
}


StoragePtr TableFunctionMerge::executeImpl(const ASTPtr & ast_function, const Context & context) const
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

    args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);
    args[1] = evaluateConstantExpressionAsLiteral(args[1], context);

    String source_database = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    String table_name_regexp = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    auto res = StorageMerge::create(
        getName(),
        ColumnsDescription{chooseColumns(source_database, table_name_regexp, context)},
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
