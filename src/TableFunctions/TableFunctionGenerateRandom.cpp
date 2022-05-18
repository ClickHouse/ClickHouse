#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageGenerateRandom.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionGenerateRandom.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

void TableFunctionGenerateRandom::parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception("Table function '" + getName() + "' requires at least one argument: "
                        " structure, [random_seed, max_string_length, max_array_length].",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (args.size() > 4)
        throw Exception("Table function '" + getName() + "' requires at most four arguments: "
                        " structure, [random_seed, max_string_length, max_array_length].",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    // All the arguments must be literals.
    for (const auto & arg : args)
    {
        if (!arg->as<const ASTLiteral>())
        {
            throw Exception(fmt::format(
                "All arguments of table function '{}' must be literals. "
                "Got '{}' instead", getName(), arg->formatForErrorMessage()),
                ErrorCodes::BAD_ARGUMENTS);
        }
    }

    /// Parsing first argument as table structure and creating a sample block
    structure = args[0]->as<const ASTLiteral &>().value.safeGet<String>();

    if (args.size() >= 2)
    {
        const Field & value = args[1]->as<const ASTLiteral &>().value;
        if (!value.isNull())
            random_seed = value.safeGet<UInt64>();
    }

    if (args.size() >= 3)
        max_string_length = args[2]->as<const ASTLiteral &>().value.safeGet<UInt64>();

    if (args.size() == 4)
        max_array_length = args[3]->as<const ASTLiteral &>().value.safeGet<UInt64>();
}

ColumnsDescription TableFunctionGenerateRandom::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionGenerateRandom::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = std::make_shared<StorageGenerateRandom>(
        StorageID(getDatabaseName(), table_name), columns, String{}, max_array_length, max_string_length, random_seed);
    res->startup();
    return res;
}

void registerTableFunctionGenerate(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateRandom>();
}

}


