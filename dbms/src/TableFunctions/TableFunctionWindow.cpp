#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageWindow.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionWindow.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static String evaluateArgument(const Context & context, ASTPtr & argument)
{
    return evaluateConstantExpressionOrIdentifierAsLiteral(argument, context)->as<ASTLiteral &>().value.safeGet<String>();
}

StoragePtr
TableFunctionWindow::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1)
            throw Exception("Table function '" + getName() + "' requires 'table name'", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String window_name = evaluateArgument(context, arguments[0]);
        auto table = context.getTable("", window_name);

        auto * window = dynamic_cast<StorageWindow *>(table.get());
        if (!window)
            throw Exception("Table function '" + getName() + "' requires StorageWindow", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto res = StorageWindowWrapper::create(table_name, *window);
        return res;
    }
    throw Exception("Table function '" + getName() + "' requires 'table name'", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void registerTableFunctionWindow(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionWindow>();
}

}
