#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
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

StoragePtr TableFunctionView::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        if (auto * select = function->tryGetQueryArgument())
        {
            auto sample = InterpreterSelectWithUnionQuery::getSampleBlock(function->arguments->children[0] /* ASTPtr */, context);
            auto columns = ColumnsDescription(sample.getNamesAndTypesList());
            ASTCreateQuery create;
            create.select = select;
            auto res = StorageView::create(StorageID(getDatabaseName(), table_name), create, columns);
            res->startup();
            return res;
        }
    }
    throw Exception("Table function '" + getName() + "' requires a query argument.", ErrorCodes::BAD_ARGUMENTS);
}

void registerTableFunctionView(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionView>();
}

}
