
#include <TableFunctions/TableFunctionRedis.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include "registerTableFunctions.h"
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/logger_useful.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StoragePtr TableFunctionRedis::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto storage = std::make_shared<StorageRedis>( 
    StorageID(getDatabaseName(), table_name), 
    configuration->host,
    configuration->port,
    configuration->db_index,
    configuration->password,
    static_cast<RedisStorageType>(configuration->storage_type),
    "primary_key",
    "secondary_key",
    "value",
    context,
    configuration->options,
    columns,
    ConstraintsDescription(),
    String{});
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionRedis::getActualTableStructure(ContextPtr context) const
{
    String structure;
    if (configuration->storage_type ==  StorageRedisConfiguration::RedisStorageType::HASH_MAP)
        structure = "primary_key String, secondary_key String, value String";
    else if (configuration->storage_type ==  StorageRedisConfiguration::RedisStorageType::SIMPLE)
        structure = "key String, value String";
    return parseColumnsListFromString(structure, context);
}

void TableFunctionRedis::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments) 
        throw Exception("Table function 'redis' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTs & args = func_args.arguments->children;

    if (args.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Table function 'redis' requires 4 parameters: redis('host:port', db_id, 'password', 'storage_type')");

    configuration = StorageRedis::getConfiguration(args, context);
}


void registerTableFunctionRedis(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionRedis>();
}

}
