#include <Parsers/ASTSQLClusterQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSQLClusterQuery.h>


namespace DB
{

namespace
{

bool parsePropertiesList(SettingsChanges & properties, IParser::Pos & pos, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    while (true)
    {
        if (!properties.empty() && !s_comma.ignore(pos, expected))
            break;

        properties.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(properties.back(), pos, expected))
            return false;
    }

    return true;
}

bool parseReplica(IParser::Pos & pos, Expected & expected, ASTPtr & replica)
{
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    if (!s_replica.ignore(pos, expected))
        return false;
    if (!s_lparen.ignore(pos, expected))
        return false;

    auto replica_ast = make_intrusive<ASTSQLClusterReplica>();
    if (!parsePropertiesList(replica_ast->properties, pos, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    replica = replica_ast;
    return true;
}

bool parseShard(IParser::Pos & pos, Expected & expected, ASTPtr & shard)
{
    ParserKeyword s_shard(Keyword::SHARD);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);

    if (!s_shard.ignore(pos, expected))
        return false;
    if (!s_lparen.ignore(pos, expected))
        return false;

    auto shard_ast = make_intrusive<ASTSQLClusterShard>();

    while (!pos->isEnd())
    {
        if (s_rparen.ignore(pos, expected))
            break;

        if (!shard_ast->properties.empty() || !shard_ast->replicas.empty())
        {
            if (!s_comma.ignore(pos, expected))
                return false;
        }

        ASTPtr replica;
        if (parseReplica(pos, expected, replica))
        {
            shard_ast->replicas.push_back(replica);
            continue;
        }

        if (!shard_ast->replicas.empty())
            return false;

        shard_ast->properties.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(shard_ast->properties.back(), pos, expected))
            return false;
    }

    shard = shard_ast;
    return true;
}

bool parseClusterDefinition(IParser::Pos & pos, Expected & expected, ASTPtr & definition)
{
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);

    if (!s_lparen.ignore(pos, expected))
        return false;

    auto definition_ast = make_intrusive<ASTSQLClusterDefinition>();

    while (!pos->isEnd())
    {
        if (s_rparen.ignore(pos, expected))
            break;

        if (!definition_ast->cluster_properties.empty() || !definition_ast->shards.empty())
        {
            if (!s_comma.ignore(pos, expected))
                return false;
        }

        ASTPtr shard;
        if (parseShard(pos, expected, shard))
        {
            definition_ast->shards.push_back(shard);
            continue;
        }

        if (!definition_ast->shards.empty())
            return false;

        definition_ast->cluster_properties.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(definition_ast->cluster_properties.back(), pos, expected))
            return false;
    }

    if (definition_ast->shards.empty())
        return false;

    definition = definition_ast;
    return true;
}

}

bool ParserCreateSQLClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    ASTPtr definition;
    if (!parseClusterDefinition(pos, expected, definition))
        return false;

    auto query = make_intrusive<ASTCreateSQLClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    query->definition = definition;
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

bool ParserAlterSQLClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_alter.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    ASTPtr definition;
    if (!parseClusterDefinition(pos, expected, definition))
        return false;

    auto query = make_intrusive<ASTAlterSQLClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    query->definition = definition;
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

bool ParserDropSQLClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTDropSQLClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

std::map<String, Documentation> ParserCreateSQLClusterQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["CREATE CLUSTER"] =
    {
        .description = R"DOCS_MD(
Creates a SQL-managed cluster and stores its definition in [SQL cluster metadata storage](/concepts/features/configuration/server-config/sql-clusters#storing-sql-cluster-metadata).

## Syntax {#syntax}

```sql
CREATE CLUSTER [IF NOT EXISTS] name [ON CLUSTER cluster] (
    cluster_property = value,
    SHARD (
        shard_property = value,
        REPLICA (replica_property = value, ...),
        ...
    ),
    SHARD (
        host = 'host', port = 9000, ...
    ),
    ...
)
```

## Description {#description}

`CREATE CLUSTER` defines a new cluster topology and materializes it in server memory. The full statement is persisted as metadata and reloaded on server startup.

Cluster names must not contain dots.

### Cluster properties {#cluster-properties}

| Property | Description |
|----------|-------------|
| `user` | Default user for replicas. Can be overridden at the shard or replica level. If omitted everywhere, defaults to `'default'`. See [inheritance rules](/concepts/features/configuration/server-config/sql-clusters#user-and-password-inheritance). |
| `password` | Default password for replicas. Can be overridden at the shard or replica level. If omitted everywhere, defaults to an empty string. See [inheritance rules](/concepts/features/configuration/server-config/sql-clusters#user-and-password-inheritance). |
| `secret` | Inter-server secret for authenticated distributed queries |
| `allow_distributed_ddl_queries` | Whether `ON CLUSTER` DDL is allowed for this cluster. Default: `true` |

### Shard properties {#shard-properties}

In a `SHARD` with `REPLICA` blocks, `user` and `password` set shard-level defaults that override cluster defaults for all replicas in that shard.

| Property | Description |
|----------|-------------|
| `weight` | Relative weight of the shard when writing through a `Distributed` table. Default: `1` |
| `internal_replication` | Whether the shard replicates data internally. Default: `false` |
| `user`, `password` | Default credentials for replicas in this shard when the shard contains `REPLICA` blocks |
| `host`, `port`, ... | When the shard has no `REPLICA` blocks, these define a single-replica shard |

### Replica properties {#replica-properties}

Replica-level `user` and `password` override shard- and cluster-level defaults for that replica.

| Property | Description |
|----------|-------------|
| `host` | Host name or IP address |
| `port` | TCP port |
| `user` | User name |
| `password` | Password |
| `secure` | Use TLS |
| `compression` | Enable compression |
| `priority` | Replica priority |
| `bind_host` | Local address to bind outgoing connections |
| `default_database` | Default database for connections to this replica |

## Access control {#access-control}

Requires the global `CREATE CLUSTER` grant (`GRANT CREATE CLUSTER ON *.*`).

## ON CLUSTER {#on-cluster}

An optional `ON CLUSTER` clause runs the statement through the [distributed DDL](/reference/statements/distributed-ddl) mechanism on all nodes of the specified ClickHouse cluster.

With [local metadata storage](/concepts/features/configuration/server-config/sql-clusters#local-storage-example), `ON CLUSTER` is required to create the metadata file on every node.

With [Keeper metadata storage](/concepts/features/configuration/server-config/sql-clusters#keeper-storage-example), metadata is shared centrally and a single-node `CREATE CLUSTER` is sufficient.

## Example {#example}

```sql
CREATE CLUSTER my_cluster (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000)
    ),
    SHARD (
        host = '127.0.0.3', port = 9000
    )
);
```

Distributed DDL example for local storage:

```sql
CREATE CLUSTER my_cluster ON CLUSTER 'main' (
    user = 'default',
    SHARD (
        REPLICA (host = 'host1', port = 9000),
        REPLICA (host = 'host2', port = 9000)
    )
);
```

## Related statements {#related-statements}

- [ALTER CLUSTER](/reference/statements/alter/cluster)
- [DROP CLUSTER](/reference/statements/drop#drop-cluster)

## See also {#see-also}

- [SQL-managed clusters guide](/concepts/features/configuration/server-config/sql-clusters)
- [`system.clusters`](/reference/system-tables/clusters)
)DOCS_MD",
        .syntax = R"(
CREATE CLUSTER [IF NOT EXISTS] name [ON CLUSTER cluster] (cluster_property = value, ..., SHARD (shard_property = value, ..., REPLICA (replica_property = value, ...), ...), ...)
)",
        .parent = "CREATE",
        .related = {"ALTER CLUSTER", "DROP"},
    };

    return documentation;
}

std::map<String, Documentation> ParserAlterSQLClusterQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["ALTER CLUSTER"] =
    {
        .description = R"DOCS_MD(
Replaces the definition of an existing SQL-managed cluster.

## Syntax {#syntax}

```sql
ALTER CLUSTER [IF EXISTS] name [ON CLUSTER cluster] (
    cluster_property = value,
    SHARD (
        shard_property = value,
        REPLICA (replica_property = value, ...),
        ...
    ),
    ...
)
```

## Description {#description}

`ALTER CLUSTER` replaces the entire cluster topology. Incremental updates are not supported: you cannot add or remove individual shards or replicas, or change only part of the definition. Provide the full target topology in every `ALTER CLUSTER` statement. The new definition is stored as a `CREATE CLUSTER` statement in [SQL cluster metadata storage](/concepts/features/configuration/server-config/sql-clusters#storing-sql-cluster-metadata) and applied immediately.

Property names and semantics are the same as in [`CREATE CLUSTER`](/reference/statements/create/cluster).

## Access control {#access-control}

Requires the global `ALTER CLUSTER` grant (`GRANT ALTER CLUSTER ON *.*`).

## ON CLUSTER {#on-cluster}

An optional `ON CLUSTER` clause runs the statement through the [distributed DDL](/reference/statements/distributed-ddl) mechanism.

With [local metadata storage](/concepts/features/configuration/server-config/sql-clusters#local-storage-example), use `ON CLUSTER` to update metadata on every node.

With [Keeper metadata storage](/concepts/features/configuration/server-config/sql-clusters#keeper-storage-example), a single-node `ALTER CLUSTER` is sufficient.

## Example {#example}

```sql
CREATE CLUSTER my_cluster (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    )
);

ALTER CLUSTER my_cluster (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000)
    )
);
```

## Related statements {#related-statements}

- [CREATE CLUSTER](/reference/statements/create/cluster)
- [DROP CLUSTER](/reference/statements/drop#drop-cluster)

## See also {#see-also}

- [SQL-managed clusters guide](/concepts/features/configuration/server-config/sql-clusters)
)DOCS_MD",
        .syntax = R"(
ALTER CLUSTER [IF EXISTS] name [ON CLUSTER cluster] (cluster_property = value, ..., SHARD (shard_property = value, ..., REPLICA (replica_property = value, ...), ...), ...)
)",
        .parent = "ALTER",
        .related = {"CREATE CLUSTER", "DROP"},
    };

    return documentation;
}

}
