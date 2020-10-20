#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionView.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void TableFunctionView::parseArguments(const ASTPtr & ast_function, const Context & /*context*/)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function)
    {
        if (auto * select = function->tryGetQueryArgument())
        {
            create.set(create.select, select->clone());
            return;
        }
    }
    throw Exception("Table function '" + getName() + "' requires a query argument.", ErrorCodes::BAD_ARGUMENTS);
}

ColumnsDescription TableFunctionView::getActualTableStructure(const Context & context) const
{
    assert(create.select);
    assert(create.children.size() == 1);
    assert(create.children[0]->as<ASTSelectWithUnionQuery>());
    auto sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], context);
    return ColumnsDescription(sample.getNamesAndTypesList());
}

StoragePtr TableFunctionView::executeImpl(const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = StorageView::create(StorageID(getDatabaseName(), table_name), create, columns);
    res->startup();
    return res;
}

void registerTableFunctionView(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionView>();
}

}
