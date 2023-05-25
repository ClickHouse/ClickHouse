#include <TableFunctions/TableFunctionRedis.h>

#include <Common/Exception.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_REDIS_STORAGE_TYPE;
}


StoragePtr TableFunctionRedis::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    checkRedisTableStructure(columns, *configuration);

    auto storage = std::make_shared<StorageRedis>(
        StorageID(toString(configuration->db_index), table_name),
        *configuration,
        columns,
        ConstraintsDescription(),
        String{});
    storage->startup();
    return storage;
}

/// TODO support user customized table structure
ColumnsDescription TableFunctionRedis::getActualTableStructure(ContextPtr context) const
{
    /// generate table structure by storage type.
    String structure;
    switch (configuration->storage_type)
    {
        case RedisStorageType::SIMPLE:
            structure = "key String, value String";
            break;
        case RedisStorageType::HASH_MAP:
            structure = "key String, field String, value String";
            break;
        case RedisStorageType::UNKNOWN:
            throw Exception(ErrorCodes::INVALID_REDIS_STORAGE_TYPE, "Invalid Redis storage type.");
    }
    return parseColumnsListFromString(structure, context);
}

void TableFunctionRedis::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'redis' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 5)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function 'Redis' requires from 5 parameters: "
            "redis('host:port', db_index, 'password', 'storage_type', 'pool_size')");
    }
    configuration = getRedisConfiguration(args, context);
}


void registerTableFunctionRedis(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionRedis>();
}

}
