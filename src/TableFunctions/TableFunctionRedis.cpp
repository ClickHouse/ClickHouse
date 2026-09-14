#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/parseAddress.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <IO/WriteHelpers.h>

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

    const char * getStorageEngineName() const override { return "Redis"; }

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

    String db_name = fmt::format("redis{}_db_{}", getDatabaseName(), configuration.db_index);
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
    factory.registerFunction<TableFunctionRedis>({.description = R"DOCS_MD(
This table function allows integrating ClickHouse with [Redis](https://redis.io/).

## Syntax {#syntax}

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

## Arguments {#arguments}

| Argument    | Description                                                                                                |
|-------------|------------------------------------------------------------------------------------------------------------|
| `host:port` | Redis server address, you can ignore port and default Redis port 6379 will be used.                          |
| `key`       | any column name in the column list.                                                                        |
| `structure` | The schema for the ClickHouse table returned from this function.                                             |
| `db_index`  | Redis db index range from 0 to 15, default is 0.                                                             |
| `password`  | User password, default is blank string.                                                                    |
| `pool_size` | Redis max connection pool size, default is 16.                                                               |
| `primary`   | must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a Redis key. |

- columns other than the primary key will be serialized in binary as Redis value in corresponding order.
- queries with key equals or in filtering will be optimized to multi keys lookup from Redis. If queries without filtering key full table scan will happen which is a heavy operation.

[Named collections](/concepts/features/configuration/server-config/named-collections) are not supported for `redis` table function at the moment.

## Returned value {#returned_value}

A table object with key as Redis key, other columns packaged together as Redis value.

## Usage Example {#usage-example}

Read from Redis:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Insert into Redis:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

## Related {#related}

- [The `Redis` table engine](/reference/engines/table-engines/integrations/redis)
- [Using redis as a dictionary source](/reference/statements/create/dictionary/sources/redis)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
