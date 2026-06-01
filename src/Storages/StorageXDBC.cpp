#include <Storages/StorageXDBC.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Net/HTTPRequest.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsBool odbc_bridge_use_connection_pooling;
}

namespace ServerSetting
{
    extern const ServerSettingsSeconds keep_alive_timeout;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


StorageXDBC::StorageXDBC(
    const StorageID & table_id_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    ColumnsDescription columns_,
    ConstraintsDescription constraints_,
    const String & comment,
    ContextPtr context_,
    const BridgeHelperPtr bridge_helper_)
    : IStorageURLBase(
        "",
        context_,
        table_id_,
        IXDBCBridgeHelper::DEFAULT_FORMAT,
        getFormatSettings(context_),
        columns_,
        constraints_,
        comment,
        "" /* CompressionMethod */)
    , bridge_helper(bridge_helper_)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , log(getLogger("Storage" + bridge_helper->getName()))
{
    uri = bridge_helper->getMainURI().toString();
}

std::string StorageXDBC::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_POST;
}

std::vector<std::pair<std::string, std::string>> StorageXDBC::getReadURIParams(
    const Names & /* column_names */,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    const ContextPtr & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size) const
{
    return bridge_helper->getURLParams(max_block_size);
}

std::function<void(std::ostream &)> StorageXDBC::getReadPOSTDataCallback(
    const Names & column_names,
    const ColumnsDescription & columns_description,
    const SelectQueryInfo & query_info,
    const ContextPtr & local_context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    String query = transformQueryForExternalDatabase(
        query_info,
        column_names,
        columns_description.getOrdinary(),
        bridge_helper->getIdentifierQuotingStyle(),
        LiteralEscapingStyle::Regular,
        remote_database_name,
        remote_table_name,
        local_context);
    LOG_TRACE(log, "Query: {}", query);

    NamesAndTypesList cols;
    for (const String & name : column_names)
    {
        auto column_data = columns_description.getPhysical(name);
        cols.emplace_back(column_data.name, column_data.type);
    }

    auto write_body_callback = [query, cols](std::ostream & os)
    {
        os << "sample_block=" << escapeForFileName(cols.toString());
        os << "&";
        os << "query=" << escapeForFileName(query);
    };

    return write_body_callback;
}

void StorageXDBC::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    bridge_helper->startBridgeSync();
    IStorageURLBase::read(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageXDBC::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    bridge_helper->startBridgeSync();

    auto request_uri = Poco::URI(uri);
    request_uri.setPath("/write");

    auto url_params = bridge_helper->getURLParams(65536);
    for (const auto & [param, value] : url_params)
        request_uri.addQueryParameter(param, value);

    request_uri.addQueryParameter("db_name", remote_database_name);
    request_uri.addQueryParameter("table_name", remote_table_name);
    request_uri.addQueryParameter("format_name", format_name);
    request_uri.addQueryParameter("sample_block", metadata_snapshot->getSampleBlock().getNamesAndTypesList().toString());


    return std::make_shared<StorageURLSink>(
        request_uri.toString(),
        format_name,
        getFormatSettings(local_context),
        metadata_snapshot->getSampleBlock(),
        local_context,
        ConnectionTimeouts::getHTTPTimeouts(
            local_context->getSettingsRef(),
            local_context->getServerSettings()),
        compression_method);
}

bool StorageXDBC::supportsSubsetOfColumns(const ContextPtr &) const
{
    return true;
}

Block StorageXDBC::getHeaderBlock(const Names & column_names, const StorageSnapshotPtr & storage_snapshot) const
{
    return storage_snapshot->getSampleBlockForColumns(column_names);
}

std::string StorageXDBC::getName() const
{
    return bridge_helper->getName();
}

namespace
{
    template <typename BridgeHelperMixin>
    void registerXDBCStorage(StorageFactory & factory, const std::string & name)
    {
        const bool is_jdbc = Poco::toLower(name) == "jdbc";
        Documentation documentation = is_jdbc
            ? Documentation{
                .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# JDBC table engine

<CloudNotSupportedBadge/>

:::note
clickhouse-jdbc-bridge contains experimental codes and is no longer supported. It may contain reliability issues and security vulnerabilities. Use it at your own risk.
ClickHouse recommend using built-in table functions in ClickHouse which provide a better alternative for ad-hoc querying scenarios (Postgres, MySQL, MongoDB, etc).
:::

Allows ClickHouse to connect to external databases via [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

To implement the JDBC connection, ClickHouse uses the separate program [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) that should run as a daemon.

This engine supports the [Nullable](../../../sql-reference/data-types/nullable.md) data type.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**Engine Parameters**

- `datasource` — URI or name of an external DBMS.

    URI Format: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    Example for MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

- `external_database` — Name of a database in an external DBMS, or, instead, an explicitly defined table schema (see examples).

- `external_table` — Name of the table in an external database or a select query like `select * from table1 where column1=1`.

- These parameters can also be passed using [named collections](operations/named-collections.md).

## Usage example {#usage-example}

Creating a table in MySQL server by connecting directly with it's console client:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Creating a table in ClickHouse server and selecting data from it:

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

## See also {#see-also}

- [JDBC table function](../../../sql-reference/table-functions/jdbc.md).
)DOCS_MD",
                .syntax = "ENGINE = JDBC('datasource', 'external_database', 'external_table')",
                .related = {"ODBC"}}
            : Documentation{
                .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# ODBC table engine

<CloudNotSupportedBadge/>

Allows ClickHouse to connect to external databases via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

To safely implement ODBC connections, ClickHouse uses a separate program `clickhouse-odbc-bridge`. If the ODBC driver is loaded directly from `clickhouse-server`, driver problems can crash the ClickHouse server. ClickHouse automatically starts `clickhouse-odbc-bridge` when it is required. The ODBC bridge program is installed from the same package as the `clickhouse-server`.

This engine supports the [Nullable](../../../sql-reference/data-types/nullable.md) data type.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(datasource, external_database, external_table)
```

See a detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

The table structure can differ from the source table structure:

- Column names should be the same as in the source table, but you can use just some of these columns and in any order.
- Column types may differ from those in the source table. ClickHouse tries to [cast](/sql-reference/functions/type-conversion-functions#CAST) values to the ClickHouse data types.
- The [external_table_functions_use_nulls](/operations/settings/settings#external_table_functions_use_nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

**Engine Parameters**

- `datasource` — Name of the section with connection settings in the `odbc.ini` file.
- `external_database` — Name of a database in an external DBMS.
- `external_table` — Name of a table in the `external_database`.

These parameters can also be passed using [named collections](operations/named-collections.md).

## Usage example {#usage-example}

**Retrieving data from the local MySQL installation via ODBC**

This example is checked for Ubuntu Linux 18.04 and MySQL server 5.7.

Ensure that unixODBC and MySQL Connector are installed.

By default (if installed from packages), ClickHouse starts as user `clickhouse`. Thus, you need to create and configure this user in the MySQL server.

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'localhost' WITH GRANT OPTION;
```

Then configure the connection in `/etc/odbc.ini`.

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USER = clickhouse
PASSWORD = clickhouse
```

You can check the connection using the `isql` utility from the unixODBC installation.

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

Table in MySQL:

```text
mysql> CREATE DATABASE test;
Query OK, 1 row affected (0,01 sec)

mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test.test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test.test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Table in ClickHouse, retrieving data from the MySQL table:

```sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

```sql
SELECT * FROM odbc_t
```

```text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## See also {#see-also}

- [ODBC dictionaries](/sql-reference/statements/create/dictionary/sources/odbc)
- [ODBC table function](../../../sql-reference/table-functions/odbc.md)
)DOCS_MD",
                .syntax = "ENGINE = ODBC('connection_settings', 'external_database', 'external_table')",
                .related = {"JDBC"}};

        factory.registerStorage(name, [name](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;

            String connection_string;
            String database_or_schema;
            String table;

            if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
            {
                if (Poco::toLower(name) == "jdbc")
                {
                    validateNamedCollection<>(*named_collection, {"datasource"}, {"schema", "external_database",
                                                                                  "external_table", "table"});

                    /// There are aliases for better compatibility and similarity between JDBC and ODBC
                    /// Both aliases cannot be specified simultaneously.

                    connection_string = named_collection->get<String>("datasource");

                    if (named_collection->has("external_database") == named_collection->has("schema"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Table function '{0}' must have exactly one `external_database` / `schema` argument", name);
                    database_or_schema = named_collection->getAny<String>({"external_database", "schema"});

                    if (named_collection->has("external_table") == named_collection->has("table"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Table function '{0}' must have exactly one `external_table` / `table` argument", name);
                    table = named_collection->getAny<String>({"external_table", "table"});
                }
                else
                {
                    validateNamedCollection<>(*named_collection, {"external_database", "external_table"}, {"datasource", "connection_settings"});

                    if (named_collection->has("datasource") == named_collection->has("connection_settings"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Table function '{0}' must have exactly one `datasource` / `connection_settings` argument", name);
                    connection_string = named_collection->getAny<String>({"datasource", "connection_settings"});

                    database_or_schema = named_collection->get<String>("external_database");
                    table = named_collection->get<String>("external_table");
                }
            }
            else
            {
                if (engine_args.size() != 3)
                    throw Exception(
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Storage {} requires exactly 3 parameters: {}('DSN', database or schema, table)",
                        name,
                        name);

                for (size_t i = 0; i < 3; ++i)
                    engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.getLocalContext());

                connection_string = checkAndGetLiteralArgument<String>(engine_args[0], "datasource");
                database_or_schema = checkAndGetLiteralArgument<String>(engine_args[1], "external_database");
                table = checkAndGetLiteralArgument<String>(engine_args[2], "external_table");
            }

            BridgeHelperPtr bridge_helper = std::make_shared<XDBCBridgeHelper<BridgeHelperMixin>>(
                args.getContext(),
                Poco::Timespan(args.getContext()->getSettingsRef()[Setting::http_receive_timeout]),
                connection_string,
                args.getContext()->getSettingsRef()[Setting::odbc_bridge_use_connection_pooling].value);
            return std::make_shared<StorageXDBC>(
                args.table_id,
                database_or_schema,
                table,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                bridge_helper);

        },
        {
            .source_access_type = BridgeHelperMixin::getSourceAccessObject(),
        },
        documentation);
    }
}

void registerStorageJDBC(StorageFactory & factory)
{
    registerXDBCStorage<JDBCBridgeMixin>(factory, "JDBC");
}

void registerStorageODBC(StorageFactory & factory)
{
    registerXDBCStorage<ODBCBridgeMixin>(factory, "ODBC");
}
}
