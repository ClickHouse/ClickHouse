#include <TableFunctions/TableFunctionArrowFlight.h>

#if USE_ARROWFLIGHT
#include <Parsers/ASTFunction.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageArrowFlight.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionArrowFlight::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool /*is_insert_query*/) const
{
    return std::make_shared<StorageArrowFlight>(
        StorageID{"arrow_flight", table_name},
        connection,
        config.dataset_name,
        cached_columns,
        ConstraintsDescription{},
        context);
}

ColumnsDescription TableFunctionArrowFlight::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return StorageArrowFlight::getTableStructureFromData(connection, config.dataset_name, context);
}

void TableFunctionArrowFlight::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'arrowFlight' must have arguments.");

    ASTs & args = func_args.arguments->children;
    config = StorageArrowFlight::getConfiguration(args, context);

    /// ArrowFlightConnection will establish connection lazily.
    connection = std::make_shared<ArrowFlightConnection>(config);
}

void registerTableFunctionArrowFlight(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionArrowFlight>(
         {.description = R"DOCS_MD(
Allows reading from and writing to data exposed via an [Apache Arrow Flight](/concepts/features/interfaces/arrowflight) server.

**Syntax**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Arguments**

- `host:port` — Address of the Arrow Flight server. If the port is omitted, the default port `8815` is used. [String](/reference/data-types/string).
- `dataset_name` — Name of the dataset or descriptor available on the Arrow Flight server. [String](/reference/data-types/string).
- `username` — Username for basic HTTP authentication. [String](/reference/data-types/string).
- `password` — Password for basic HTTP authentication. [String](/reference/data-types/string).

If `username` and `password` are not specified, authentication is not used (this works only if the Arrow Flight server allows unauthenticated access).

The function also supports [named collections](/concepts/features/configuration/server-config/named-collections) — see the [ArrowFlight table engine](/reference/engines/table-engines/integrations/arrowflight#named-collections) for the list of supported parameters.

**Returned value**

A table object representing the remote dataset. The schema is inferred from the Arrow Flight server.

**Settings**

- `arrow_flight_request_descriptor_type` — Controls how the dataset name is sent to the Flight server. Values: `path` (default) or `command`. See the [ArrowFlight table engine](/reference/engines/table-engines/integrations/arrowflight#settings) for details.

**Examples**

Reading from a remote Arrow Flight server:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserting data into a remote Arrow Flight server:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

Using a named collection:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**See Also**

- [ArrowFlight table engine](/reference/engines/table-engines/integrations/arrowflight)
- [Arrow Flight Interface](/concepts/features/interfaces/arrowflight)
- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {});

    /// "arrowflight" is an obsolete name.
    factory.registerAlias("arrowflight", "arrowFlight");
}

}

#endif
