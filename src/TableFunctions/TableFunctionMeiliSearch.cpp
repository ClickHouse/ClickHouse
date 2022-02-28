#include <memory>
#include <TableFunctions/TableFunctionMeiliSearch.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/MeiliSearch/StorageMeiliSearch.h>
#include <Storages/MeiliSearch/MeiliSearchColumnDescriptionFetcher.h>
#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

StoragePtr TableFunctionMeiliSearch::executeImpl(const ASTPtr & /* ast_function */,
        ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    return StorageMeiliSearch::create(
        StorageID(getDatabaseName(), table_name), 
        configuration.value(), 
        columns, 
        ConstraintsDescription{},
        String{});
}

ColumnsDescription TableFunctionMeiliSearch::getActualTableStructure(ContextPtr /* context */) const
{
    MeiliSearchColumnDescriptionFetcher fetcher(configuration.value());
    fetcher.addParam(doubleQuoteString("limit"), "1");
    return fetcher.fetchColumnsDescription();
}


void TableFunctionMeiliSearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    configuration = StorageMeiliSearch::getConfiguration(func_args.arguments->children, context);
}

void registerTableFunctionMeiliSearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMeiliSearch>();
}

}
