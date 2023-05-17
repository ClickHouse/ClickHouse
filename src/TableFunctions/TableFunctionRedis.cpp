#include <TableFunctions/TableFunctionRedis.h>

#include <Common/Exception.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StoragePtr TableFunctionRedis::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto storage = std::make_shared<StorageRedis>(
        StorageID(configuration->db_id, table_name), configuration, columns, ConstraintsDescription(), String{});// TODO
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionRedis::getActualTableStructure(ContextPtr context) const
{
    /// generate table structure by storage type.
    String structure;
    switch (configuration->storage_type)
    {
        case StorageRedis::StorageType::SIMPLE:
            structure = "key String, value String";
            break;
        case StorageRedis::StorageType::HASH:
            structure = "key String, field, String, value String";
            break;
        case StorageRedis::StorageType::LIST:
            structure = "key String, value Array(String)";
            break;
        case StorageRedis::StorageType::SET:
            structure = "key String, value Array(String)";
            break;
        case StorageRedis::StorageType::ZSET:
            structure = "key String, value Array(String)";
            break;
    }
    return parseColumnsListFromString(structure, context);
}

void TableFunctionRedis::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'redis' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 4)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function 'Redis' requires from 4 parameters: "
            "redis('host:port', db_id, 'password', 'storage_type')");
    }
    configuration = StorageRedis::getConfiguration(args, context);
}


void registerTableFunctionRedis(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionRedis>();
}

}
