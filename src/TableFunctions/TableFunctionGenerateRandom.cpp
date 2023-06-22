#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageGenerateRandom.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionGenerateRandom.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>

#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

void TableFunctionGenerateRandom::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        return;

    if (args.size() > 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires at most four arguments: "
                        " structure, [random_seed, max_string_length, max_array_length].", getName());

    /// Allow constant expression for structure argument, it can be generated using generateRandomStructure function.
    args[0] = evaluateConstantExpressionAsLiteral(args[0], context);

    // All the arguments must be literals.
    for (const auto & arg : args)
    {
        if (!arg->as<const ASTLiteral>())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "All arguments of table function '{}' must be literals. "
                "Got '{}' instead", getName(), arg->formatForErrorMessage());
        }
    }

    /// Parsing first argument as table structure and creating a sample block
    structure = checkAndGetLiteralArgument<String>(args[0], "structure");

    if (args.size() >= 2)
    {
        const auto & literal = args[1]->as<const ASTLiteral &>();
        if (!literal.value.isNull())
            random_seed = checkAndGetLiteralArgument<UInt64>(literal, "random_seed");
    }

    if (args.size() >= 3)
        max_string_length = checkAndGetLiteralArgument<UInt64>(args[2], "max_string_length");

    if (args.size() == 4)
        max_array_length = checkAndGetLiteralArgument<UInt64>(args[3], "max_string_length");
}

ColumnsDescription TableFunctionGenerateRandom::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
    {
        if (structure_hint.empty())
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Table function '{}' was used without structure argument but structure could not be determined automatically. Please, "
                "provide structure manually",
                getName());
        return structure_hint;
    }

    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionGenerateRandom::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    ColumnsDescription columns = getActualTableStructure(context);
    auto res = std::make_shared<StorageGenerateRandom>(
        StorageID(getDatabaseName(), table_name), columns, String{}, max_array_length, max_string_length, random_seed);
    res->startup();
    return res;
}

void registerTableFunctionGenerate(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateRandom>({.documentation = {}, .allow_readonly = true});
}

}


