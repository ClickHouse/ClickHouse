#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/parseAddress.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Storages/StorageRedis.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/* Implements Redis table function.
 * Use redis(host:port, key, structure[, db_index[, password[, pool_size]]]);
 */
class TableFunctionRedis : public ITableFunction
{
public:
    static constexpr auto name = "redis";
    String getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context,
        const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Redis"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    RedisConfiguration configuration;
    String structure;
    String primary_key;
};

StoragePtr TableFunctionRedis::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);

    String db_name = "redis" + getDatabaseName() + "_db_" + toString(configuration.db_index);
    auto storage = std::make_shared<StorageRedis>(
        StorageID(db_name, table_name), configuration, context, metadata, primary_key);
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionRedis::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(structure, context);
}

void TableFunctionRedis::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'redis' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() < 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad arguments count when creating Redis table function");

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(args[0], "host:port"), 6379);
    configuration.host = parsed_host_port.first;
    configuration.port = parsed_host_port.second;

    primary_key = checkAndGetLiteralArgument<String>(args[1], "key");
    structure = checkAndGetLiteralArgument<String>(args[2], "structure");

    if (args.size() > 3)
        configuration.db_index = static_cast<uint32_t>(checkAndGetLiteralArgument<UInt64>(args[3], "db_index"));
    else
        configuration.db_index = DEFAULT_REDIS_DB_INDEX;
    if (args.size() > 4)
        configuration.password = checkAndGetLiteralArgument<String>(args[4], "password");
    else
        configuration.password = DEFAULT_REDIS_PASSWORD;
    if (args.size() > 5)
        configuration.pool_size = static_cast<uint32_t>(checkAndGetLiteralArgument<UInt64>(args[5], "pool_size"));
    else
        configuration.pool_size = DEFAULT_REDIS_POOL_SIZE;

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    auto columns = parseColumnsListFromString(structure, context);
    if (!columns.has(primary_key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad arguments redis table function structure should contains key.");
}

}

void registerTableFunctionRedis(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionRedis>();
}

}
