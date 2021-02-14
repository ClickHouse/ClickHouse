#include <TableFunctions/TableFunctionProjection.h>

#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageProjection.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static NamesAndTypesList chooseColumns(const StorageID & source_table, const String & projection_name, const Context & context)
{
    auto storage = DatabaseCatalog::instance().getTable(source_table, context);

    const auto & projections = storage->getInMemoryMetadataPtr()->projections;
    return projections.get(projection_name).sample_block.getNamesAndTypesList();
}

void TableFunctionProjection::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(
            "Table function `projection` requires exactly 2 arguments"
            " - names of source table and projection",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2)
        throw Exception(
            "Table function `projection` requires exactly 2 arguments"
            " - names of source table and projection",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (const auto * ident = args[0]->as<ASTIdentifier>())
        source_table_id = IdentifierSemantic::extractDatabaseAndTable(*ident);
    else
        throw Exception(
            "The first argument of table function `projection` should be an identifier"
            " - name of source table",
            ErrorCodes::BAD_ARGUMENTS);

    if (source_table_id.database_name.empty())
        source_table_id.database_name = context.getConfigRef().getString("default_database", "default");
    projection_name = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context)->as<const ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionProjection::getActualTableStructure(const Context & context) const
{
    return ColumnsDescription{chooseColumns(source_table_id, projection_name, context)};
}

StoragePtr TableFunctionProjection::executeImpl(
    const ASTPtr &, const Context & context, const std::string & table_name, ColumnsDescription /* cached_columns */) const
{
    auto res = StorageProjection::create(
        StorageID(getDatabaseName(), table_name), getActualTableStructure(context), source_table_id, projection_name, context);
    res->startup();
    return res;
}

void registerTableFunctionProjection(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionProjection>();
}

}
