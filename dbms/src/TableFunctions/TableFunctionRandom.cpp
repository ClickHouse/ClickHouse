#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageValues.h>
#include <DataTypes/DataTypeTuple.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionRandom.h>
#include <TableFunctions/TableFunctionFactory.h>

#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionRandom::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() > 2)
        throw Exception("Table function '" + getName() + "' requires one or two arguments: structure (and limit).",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// Parsing first argument as table structure and creating a sample block
    std::string structure = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    UInt64 limit = 1;
    /// Parsing second argument if present
    if (args.size() == 2)
        limit = args[1]->as<ASTLiteral &>().value.safeGet<Uint64>();

    if (!limit)
        throw Exception("Table function '" + getName() + "' limit should not be 0.", ErrorCodes::BAD_ARGUMENTS);

    ColumnsDescription columns = parseColumnsListFromString(structure, context);

    Block res_block;
    for (const auto & name_type : columns.getOrdinary())
        Column c = name_type.type->createColumnWithRandomData(limit) ;
        res_block.insert({ c, name_type.type, name_type.name });

    auto res = StorageValues::create(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

void registerTableFunctionRandom(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionRandom>(TableFunctionFactory::CaseInsensitive);
}

}
