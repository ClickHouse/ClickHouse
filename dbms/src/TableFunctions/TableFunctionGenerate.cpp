#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageGenerate.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionGenerate.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

StoragePtr TableFunctionGenerate::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 1)
        throw Exception("Table function '" + getName() + "' requires at least one argument: "
                        " structure(, max_array_length, max_string_length, random_seed).",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (args.size() > 4)
        throw Exception("Table function '" + getName() + "' requires at most four arguments: "
                        " structure, max_array_length, max_string_length, random_seed.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// Parsing first argument as table structure and creating a sample block
    std::string structure = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    UInt64 max_array_length = 10;
    UInt64 max_string_length = 10;
    UInt64 random_seed = 0; // zero for random

    /// Parsing second argument if present
    if (args.size() >= 2)
        max_array_length = args[1]->as<ASTLiteral &>().value.safeGet<UInt64>();

    if (args.size() >= 3)
        max_string_length = args[2]->as<ASTLiteral &>().value.safeGet<UInt64>();

    if (args.size() == 4)
        random_seed = args[3]->as<ASTLiteral &>().value.safeGet<UInt64>();

    ColumnsDescription columns = parseColumnsListFromString(structure, context);

    auto res = StorageGenerate::create(StorageID(getDatabaseName(), table_name), columns, max_array_length, max_string_length, random_seed);
    res->startup();
    return res;
}

void registerTableFunctionGenerate(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerate>(TableFunctionFactory::CaseInsensitive);
}

}


