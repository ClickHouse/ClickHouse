#include <TableFunctions/TableFunctionRemote.h>

#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Common/typeid_cast.h>
#include <Common/parseRemoteDescription.h>
#include <Common/Macros.h>
#include <Common/RemoteHostFilter.h>
#include <Common/VectorWithMemoryTracking.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Access/Common/AccessFlags.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 table_function_remote_max_addresses;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


void TableFunctionRemote::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    String cluster_name;
    String cluster_description;
    String database = "system";
    String table = "one"; /// The table containing one row is used by default for queries without explicit table specification.
    String username = "default";
    String password;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    /**
     * Number of arguments for remote function is 6.
     * Number of arguments for cluster function is 4.
     * For now named collection can be used only for remote as cluster does not require credentials.
     */
    size_t max_args = is_cluster_function ? 4 : 6;
    NamedCollectionPtr named_collection;
    VectorWithMemoryTracking<std::pair<std::string, ASTPtr>> complex_args;
    if (!is_cluster_function && (named_collection = tryGetNamedCollectionWithOverrides(args, context, false, &complex_args)))
    {
        validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(
            *named_collection,
            {"addresses_expr", "host", "hostname", "table"},
            {"username", "user", "password", "sharding_key", "port", "database", "db"});

        if (!complex_args.empty())
        {
            for (const auto & [arg_name, arg_ast] : complex_args)
            {
                if (arg_name == "database" || arg_name == "db")
                {
                    /// A table function is allowed here (remote(nc, database = mysql(...))), mirroring the
                    /// positional signature remote('addr', table_function()). Any other expression is a
                    /// database name, so a non-function node never reaches TableFunctionFactory::get().
                    const auto * function = arg_ast->as<ASTFunction>();
                    if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
                        remote_table_function_ptr = arg_ast;
                    else
                        database = checkAndGetLiteralArgument<String>(
                            evaluateConstantExpressionForDatabaseName(arg_ast, context), "database");
                }
                else if (arg_name == "sharding_key")
                    sharding_key = arg_ast;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected argument representation for {}", arg_name);
            }
        }
        else
            database = named_collection->getAnyOrDefault<String>({"db", "database"}, "default");

        cluster_description = named_collection->getOrDefault<String>("addresses_expr", "");
        if (cluster_description.empty() && named_collection->hasAny({"host", "hostname"}))
            cluster_description = named_collection->has("port")
                ? named_collection->getAny<String>({"host", "hostname"}) + ':' + toString(named_collection->get<UInt64>("port"))
                : named_collection->getAny<String>({"host", "hostname"});
        table = named_collection->get<String>("table");
        username = named_collection->getAnyOrDefault<String>({"username", "user"}, "default");
        password = named_collection->getOrDefault<String>("password", "");
    }
    else
    {
        /// Supported signatures:
        /// remote('addresses_expr')
        /// remote('addresses_expr', db.table)
        /// remote('addresses_expr', 'db', 'table')
        /// remote('addresses_expr', db.table, 'user')
        /// remote('addresses_expr', 'db', 'table', 'user')
        /// remote('addresses_expr', db.table, 'user', 'password')
        /// remote('addresses_expr', 'db', 'table', 'user', 'password')
        /// remote('addresses_expr', db.table, sharding_key)
        /// remote('addresses_expr', 'db', 'table', sharding_key)
        /// remote('addresses_expr', db.table, 'user', sharding_key)
        /// remote('addresses_expr', 'db', 'table', 'user', sharding_key)
        /// remote('addresses_expr', db.table, 'user', 'password', sharding_key)
        /// remote('addresses_expr', 'db', 'table', 'user', 'password', sharding_key)
        ///
        /// remoteSecure() - same as remote()
        ///
        /// cluster()
        /// cluster('cluster_name')
        /// cluster('cluster_name', db.table)
        /// cluster('cluster_name', 'db', 'table')
        /// cluster('cluster_name', table_function())
        /// cluster('cluster_name', db.table, sharding_key)
        /// cluster('cluster_name', 'db', 'table', sharding_key)
        /// cluster('cluster_name', table_function(), sharding_key)
        ///
        /// clusterAllReplicas() - same as cluster()

        if ((!is_cluster_function && args.empty()) || args.size() > max_args)
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        size_t arg_num = 0;
        auto get_string_literal = [](const IAST & node, String & res)
        {
            const auto * lit = node.as<ASTLiteral>();
            if (!lit)
                return false;

            if (lit->value.getType() != Field::Types::String)
                return false;

            res = lit->value.safeGet<String>();
            return true;
        };

        if (is_cluster_function)
        {
            if (!args.empty())
            {
                args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                cluster_name = checkAndGetLiteralArgument<String>(args[arg_num], "cluster_name");
            }
            else
            {
                cluster_name = "default";
            }
        }
        else
        {
            if (!tryGetIdentifierNameInto(args[arg_num], cluster_name))
            {
                if (!get_string_literal(*args[arg_num], cluster_description))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hosts pattern must be string literal (in single quotes).");
            }
        }

        ++arg_num;

        /// Names of database and table is not necessary.
        if (arg_num < args.size())
        {
            const auto * function = args[arg_num]->as<ASTFunction>();
            if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
            {
                remote_table_function_ptr = args[arg_num];
                ++arg_num;

                /// Cluster function may have sharding key for insert
                if (is_cluster_function && arg_num < args.size())
                {
                    sharding_key = args[arg_num];
                    ++arg_num;
                }
            }
            else
            {
                args[arg_num] = evaluateConstantExpressionForDatabaseName(args[arg_num], context);
                database = checkAndGetLiteralArgument<String>(args[arg_num], "database");

                ++arg_num;

                auto qualified_name = QualifiedTableName::parseFromString(database);
                if (qualified_name.database.empty())
                {
                    if (arg_num >= args.size())
                    {
                        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table name was not found in function arguments. {}", static_cast<const std::string>(help_message));
                    }

                    std::swap(qualified_name.database, qualified_name.table);
                    args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                    qualified_name.table = checkAndGetLiteralArgument<String>(args[arg_num], "table");
                    ++arg_num;
                }

                database = std::move(qualified_name.database);
                table = std::move(qualified_name.table);

                /// Cluster function may have sharding key for insert
                if (is_cluster_function && arg_num < args.size())
                {
                    sharding_key = args[arg_num];
                    ++arg_num;
                }
            }
        }

        /// Username and password parameters are prohibited in cluster version of the function.
        /// The user name accepts string literals, and additionally bare identifiers when they
        /// are followed by a string-literal password — this is consistent with how the database
        /// and table arguments accept unquoted identifiers, but avoids breaking the historical
        /// behavior of `remote('host', db.table, sharding_key)`, where a bare identifier at the
        /// same position was silently treated as a sharding key. The password must still be a
        /// string literal.
        if (!is_cluster_function)
        {
            bool password_consumed = false;
            if (arg_num < args.size())
            {
                if (get_string_literal(*args[arg_num], username))
                {
                    ++arg_num;
                }
                else if (arg_num + 1 < args.size()
                    && get_string_literal(*args[arg_num + 1], password)
                    && tryGetIdentifierNameInto(args[arg_num], username))
                {
                    arg_num += 2;
                    password_consumed = true;
                }
                else
                {
                    username = "default";
                    sharding_key = args[arg_num];
                    ++arg_num;
                }
            }

            if (arg_num < args.size() && !sharding_key && !password_consumed)
            {
                if (!get_string_literal(*args[arg_num], password))
                {
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size())
            {
                if (sharding_key)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Arguments `user` and `password` should be string literals (in single quotes)");
                sharding_key = args[arg_num];
                ++arg_num;
            }
        }

        if (arg_num < args.size())
        {
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }

    if (!cluster_name.empty())
    {
        /// Use an existing cluster from the main config
        if (name != "clusterAllReplicas")
            cluster = context->getCluster(cluster_name);
        else
            cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettingsRef());
    }
    else
    {
        /// Create new cluster from the scratch
        size_t max_addresses = context->getSettingsRef()[Setting::table_function_remote_max_addresses];
        Strings shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

        HostsByShard names;
        names.reserve(shards.size());
        for (const auto & shard : shards)
        {
            auto replicas = parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses);
            if (replicas.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard contains zero number of replicas");
            names.push_back(std::move(replicas));
        }

        if (names.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard list is empty after parsing the first argument");

        auto maybe_secure_port = context->getTCPPortSecure();

        /// Check host and port on affiliation allowed hosts.
        for (const auto & hosts : names)
        {
            for (const auto & host : hosts)
            {
                size_t colon = host.find(':');
                if (colon == String::npos)
                    context->getRemoteHostFilter().checkHostAndPort(
                        host,
                        toString((secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort())));
                else
                    context->getRemoteHostFilter().checkHostAndPort(host.substr(0, colon), host.substr(colon + 1));
            }
        }

        bool treat_local_as_remote = false;
        bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;
        ClusterConnectionParameters params{
            username,
            password,
            static_cast<UInt16>(secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
            treat_local_as_remote,
            treat_local_port_as_remote,
            secure,
            /* bind_host= */ "",
            /* priority= */ Priority{1},
            /* cluster_name= */ "",
            /* cluster_secret= */ ""
        };
        cluster = std::make_shared<Cluster>(context->getSettingsRef(), names, params);
    }

    if (!remote_table_function_ptr && table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The name of remote table cannot be empty");

    remote_table_id.database_name = database;
    remote_table_id.table_name = table;
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context, is_insert_query);

    chassert(cluster);

    bool has_local_shard = false;
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (shard_info.isLocal())
        {
            has_local_shard = true;
            break;
        }
    }

    if (has_local_shard && !is_insert_query)
        context->checkAccess(AccessType::SELECT, remote_table_id);
    else if (has_local_shard)
        context->checkAccess(AccessType::INSERT, remote_table_id);

    StoragePtr res = std::make_shared<StorageDistributed>(
            StorageID(getDatabaseName(), table_name),
            cached_columns,
            ConstraintsDescription{},
            String{},
            remote_table_id.database_name,
            remote_table_id.table_name,
            String{},
            context,
            sharding_key,
            String{},
            String{},
            DistributedSettings{},
            LoadingStrictnessLevel::CREATE,
            cluster,
            remote_table_function_ptr,
            !is_cluster_function);

    res->startup();
    return res;
}

ColumnsDescription TableFunctionRemote::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    chassert(cluster);
    return getStructureOfRemoteTable(*cluster, remote_table_id, context, remote_table_function_ptr);
}

TableFunctionRemote::TableFunctionRemote(const std::string & name_, bool secure_)
    : name{name_}, secure{secure_}
{
    is_cluster_function = (name == "cluster" || name == "clusterAllReplicas");
    help_message = PreformattedMessage::create(
        "Table function '{}' requires from {} to {} parameters: "
        "{}",
        name,
        is_cluster_function ? 0 : 1,
        is_cluster_function ? 4 : 6,
        is_cluster_function ? "[<cluster name or default if not specified>, [<database.table> | [<name of remote database>, <name of remote table>] | <table function>]] [, sharding_key]"
                            : "<addresses pattern> [, <name of remote database>, <name of remote table>] [, username[, password], sharding_key]");
}

void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); }, {.description = R"DOCS_MD(
Table function `remote` allows to access remote servers on-the-fly, i.e. without creating a [Distributed](/reference/engines/table-engines/special/distributed) table. Table function `remoteSecure` is same as `remote` but over a secure connection.

Both functions can be used in `SELECT` and `INSERT` queries.

## Syntax {#syntax}

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

## Parameters {#parameters}

| Argument       | Description                                                                                                                                                                                                                                                                                                                                                        |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `addresses_expr` | A remote server address or an expression that generates multiple addresses of remote servers. Format: `host` or `host:port`.<br/><br/>    The `host` can be specified as a server name, or as a IPv4 or IPv6 address. An IPv6 address must be specified in `[]`.<br/><br/>    The `port` is the TCP port on the remote server. If the port is omitted, it uses [tcp_port](/reference/settings/server-settings/settings#tcp_port) from the server config file for table function `remote` (by default, 9000) and [tcp_port_secure](/reference/settings/server-settings/settings#tcp_port_secure) for table function `remoteSecure` (by default, 9440).<br/><br/>    For IPv6 addresses, a port is required.<br/><br/>    If only parameter `addresses_expr` is specified, `db` and `table` will use `system.one` by default.<br/><br/>    Type: [String](/reference/data-types/string). |
| `db`           | Database name. Type: [String](/reference/data-types/string).                                                                                                                                                                                                                                                                                             |
| `table`        | Table name. Type: [String](/reference/data-types/string).                                                                                                                                                                                                                                                                                               |
| `user`         | User name. If not specified, `default` is used. Type: [String](/reference/data-types/string).                                                                                                                                                                                                                                                         |
| `password`     | User password. If not specified, an empty password is used. Type: [String](/reference/data-types/string).                                                                                                                                                                                                                                             |
| `sharding_key` | Sharding key to support distributing data across nodes. For example: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. Type: [UInt32](/reference/data-types/int-uint).                                                                                                                                                 |

Arguments also can be passed using [named collections](/concepts/features/configuration/server-config/named-collections).

## Returned value {#returned-value}

A table located on a remote server.

## Usage {#usage}

As table functions `remote` and `remoteSecure` re-establish the connection for each request, it is recommended to use a `Distributed` table instead. Also, if hostnames are set, the names are resolved, and errors are not counted when working with various replicas. When processing a large number of queries, always create the `Distributed` table ahead of time, and do not use the `remote` table function.

The `remote` table function can be useful in the following cases:

- One-time data migration from one system to another
- Accessing a specific server for data comparison, debugging, and testing, i.e. ad-hoc connections.
- Queries between various ClickHouse clusters for research purposes.
- Infrequent distributed requests that are made manually.
- Distributed requests where the set of servers is re-defined each time.

### Addresses {#addresses}

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Multiple addresses can be comma-separated. In this case, ClickHouse will use distributed processing and send the query to all specified addresses (like shards with different data). Example:

```text
example01-01-1,example01-02-1
```

## Examples {#examples}

### Selecting data from a remote server: {#selecting-data-from-a-remote-server}

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

Or using [named collections](/concepts/features/configuration/server-config/named-collections):

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

### Inserting data into a table on a remote server: {#inserting-data-into-a-table-on-a-remote-server}

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

### Migration of tables from one system to another: {#migration-of-tables-from-one-system-to-another}

This example uses one table from a sample dataset.  The database is `imdb`, and the table is `actors`.

#### On the source ClickHouse system (the system that currently hosts the data) {#on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data}

- Verify the source database and table name (`imdb.actors`)

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

- Get the CREATE TABLE statement from the source:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
  ```

  Response

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

#### On the destination ClickHouse system {#on-the-destination-clickhouse-system}

- Create the destination database:

  ```sql
  CREATE DATABASE imdb
  ```

- Using the CREATE TABLE statement from the source, create the destination:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

#### Back on the source deployment {#back-on-the-source-deployment}

Insert into the new database and table created on the remote system.  You will need the host, port, username, password, destination database, and destination table.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

## Globbing {#globs-in-addresses}

Patterns in `{ }` are used to generate a set of shards and to specify replicas. If there are multiple pairs of `{ }`, then the direct product of the corresponding sets is generated.

The following pattern types are supported.

- `{a,b,c}` - Represents any of alternative strings `a`, `b` or `c`. The pattern is replaced with `a` in the first shard address and replaced with `b` in the second shard address and so on. For instance, `example0{1,2}-1` generates addresses `example01-1` and `example02-1`.
- `{N..M}` - A range of numbers. This pattern generates shard addresses with incrementing indices from `N` to (and including) `M`. For instance, `example0{1..2}-1` generates `example01-1` and `example02-1`.
- `{0n..0m}` - A range of numbers with leading zeroes. This pattern preserves leading zeroes in indices. For instance, `example{01..03}-1` generates `example01-1`, `example02-1` and `example03-1`.
- `{a|b}` - Any number of variants separated by a `|`. The pattern specifies replicas. For instance, `example01-{1|2}` generates replicas `example01-1` and `example01-2`.

The query will be sent to the first healthy replica. However, for `remote` the replicas are iterated in the order currently set in the [load_balancing](/reference/settings/session-settings#load_balancing) setting.
The number of generated addresses is limited by [table_function_remote_max_addresses](/reference/settings/session-settings#table_function_remote_max_addresses) setting.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}});
    factory.registerFunction("remoteSecure", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote", /* secure = */ true); }, {.description = R"DOC(Like the remote table function, but establishes a TLS-encrypted (secure) connection to the remote server.)DOC", .category = FunctionDocumentation::Category::TableFunction}});
    factory.registerFunction("cluster", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("cluster"); }, {.description = R"DOCS_MD(
Allows accessing all shards (configured in the `remote_servers` section) of a cluster without creating a [Distributed](/reference/engines/table-engines/special/distributed) table. Only one replica of each shard is queried.

`clusterAllReplicas` function — same as `cluster`, but all replicas are queried. Each replica in a cluster is used as a separate shard/connection.

<Note>
All available clusters are listed in the [system.clusters](/reference/system-tables/clusters) table.
</Note>

## Syntax {#syntax}

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```
## Arguments {#arguments}

| Arguments                   | Type                                                                                                                                              |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name`              | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers, set `default` if not specified. |
| `db.table` or `db`, `table` | Name of a database and a table.                                                                                                                   |
| `sharding_key`              | A sharding key. Optional. Needs to be specified if the cluster has more than one shard.                                                           |

## Returned value {#returned_value}

The dataset from clusters.

## Using macros {#using_macros}

`cluster_name` can contain macros — substitution in `{}`. The substituted value is taken from the [macros](/reference/settings/server-settings/settings#macros) section of the server configuration file.

Example:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

## Usage and recommendations {#usage_recommendations}

Using the `cluster` and `clusterAllReplicas` table functions are less efficient than creating a `Distributed` table because in this case, the server connection is re-established for every request. When processing a large number of queries, please always create the `Distributed` table ahead of time, and do not use the `cluster` and `clusterAllReplicas` table functions.

The `cluster` and `clusterAllReplicas` table functions can be useful in the following cases:

- Accessing a specific cluster for data comparison, debugging, and testing.
- Queries to various ClickHouse clusters and replicas for research purposes.
- Infrequent distributed requests that are made manually.

Connection settings like `host`, `port`, `user`, `password`, `compression`, `secure` are taken from `<remote_servers>` config section. See details in [Distributed engine](/reference/engines/table-engines/special/distributed).

## Related {#related}

- [skip_unavailable_shards](/reference/settings/session-settings#skip_unavailable_shards)
- [load_balancing](/reference/settings/session-settings#load_balancing)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true}});
    factory.registerFunction("clusterAllReplicas", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("clusterAllReplicas"); }, {.description = R"DOC(Like the cluster table function, but queries all replicas of every shard in the cluster instead of a single replica per shard.)DOC", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true}});
}

}
