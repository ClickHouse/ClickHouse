#include <memory>
#include <Parsers/ASTFunction.h>
#include <Storages/MeiliSearch/MeiliSearchColumnDescriptionFetcher.h>
#include <Storages/MeiliSearch/StorageMeiliSearch.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionMeiliSearch.h>
#include <Common/Exception.h>

namespace DB
{
StoragePtr TableFunctionMeiliSearch::executeImpl(
    const ASTPtr & /* ast_function */, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    return std::make_shared<StorageMeiliSearch>(
        StorageID(getDatabaseName(), table_name), configuration.value(), columns, ConstraintsDescription{}, String{});
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
