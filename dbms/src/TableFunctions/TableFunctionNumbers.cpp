#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionNumbers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StoragePtr TableFunctionNumbers::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Table function 'numbers' requires 'length' or 'offset, length'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        UInt64 offset = arguments.size() == 2 ? evaluateArgument(context, arguments[0]) : 0;
        UInt64 length = arguments.size() == 2 ? evaluateArgument(context, arguments[1]) : evaluateArgument(context, arguments[0]);

        auto res = StorageSystemNumbers::create(getName(), false, length, offset);
        res->startup();
        return res;
    }
    throw Exception("Table function 'numbers' requires 'limit' or 'offset, limit'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void registerTableFunctionNumbers(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNumbers>();
}


UInt64 TableFunctionNumbers::evaluateArgument(const Context & context, ASTPtr & argument) const
{
    return evaluateConstantExpressionOrIdentifierAsLiteral(argument, context)->as<ASTLiteral &>().value.safeGet<UInt64>();
}

}
