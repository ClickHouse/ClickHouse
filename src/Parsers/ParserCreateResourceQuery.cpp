#include <Parsers/ParserCreateResourceQuery.h>

#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool parseOneOperation(ASTCreateResourceQuery::Operation & operation, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier disk_name_p;

    ResourceAccessMode mode = {};
    ASTPtr node;
    std::optional<String> disk;

    if (ParserKeyword(Keyword::WRITE).ignore(pos, expected))
        mode = ResourceAccessMode::DiskWrite;
    else if (ParserKeyword(Keyword::READ).ignore(pos, expected))
        mode = ResourceAccessMode::DiskRead;
    else if (ParserKeyword(Keyword::MASTER_THREAD).ignore(pos, expected))
        mode = ResourceAccessMode::MasterThread;
    else if (ParserKeyword(Keyword::WORKER_THREAD).ignore(pos, expected))
        mode = ResourceAccessMode::WorkerThread;
    else if (ParserKeyword(Keyword::QUERY).ignore(pos, expected))
        mode = ResourceAccessMode::Query;
    else if (ParserKeyword(Keyword::MEMORY_RESERVATION).ignore(pos, expected))
        mode = ResourceAccessMode::MemoryReservation;
    else
        return false;

    if (mode == ResourceAccessMode::DiskWrite || mode == ResourceAccessMode::DiskRead)
    {
        if (ParserKeyword(Keyword::ANY).ignore(pos, expected))
        {
            if (!ParserKeyword(Keyword::DISK).ignore(pos, expected))
                return false;
        }
        else
        {
            if (!ParserKeyword(Keyword::DISK).ignore(pos, expected))
                return false;

            if (!disk_name_p.parse(pos, node, expected))
                return false;

            disk.emplace();
            if (!tryGetIdentifierNameInto(node, *disk))
                return false;
        }
    }

    operation.mode = mode;
    operation.disk = std::move(disk);

    return true;
}

bool parseOperations(IParser::Pos & pos, Expected & expected, ASTCreateResourceQuery::Operations & operations)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        ParserToken s_open(TokenType::OpeningRoundBracket);
        ParserToken s_close(TokenType::ClosingRoundBracket);

        if (!s_open.ignore(pos, expected))
            return false;

        ASTCreateResourceQuery::Operations res_operations;

        auto parse_operation = [&]
        {
            ASTCreateResourceQuery::Operation operation;
            if (!parseOneOperation(operation, pos, expected))
                return false;
            if (!res_operations.empty() && res_operations.front().unit() != operation.unit())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Resource definition could not mix CPU and IO operations.");
            res_operations.push_back(std::move(operation));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_operation, false))
            return false;

        if (!s_close.ignore(pos, expected))
            return false;

        operations = std::move(res_operations);
        return true;
    });
}

}

bool ParserCreateResourceQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_resource(Keyword::RESOURCE);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier resource_name_p;

    ASTPtr resource_name;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_resource.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!resource_name_p.parse(pos, resource_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    ASTCreateResourceQuery::Operations operations;
    if (!parseOperations(pos, expected, operations))
        return false;

    auto create_resource_query = make_intrusive<ASTCreateResourceQuery>();
    node = create_resource_query;

    create_resource_query->resource_name = resource_name;
    create_resource_query->children.push_back(resource_name);

    create_resource_query->or_replace = or_replace;
    create_resource_query->if_not_exists = if_not_exists;
    create_resource_query->cluster = std::move(cluster_str);

    create_resource_query->unit = operations.empty() ? CostUnit::IOByte : operations.front().unit();
    create_resource_query->operations = std::move(operations);

    return true;
}

std::map<String, Documentation> ParserCreateResourceQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["CREATE RESOURCE"] =
    {
        .description = R"DOCS_MD(
Introduced in: v24.11.0

Creates a resource for [workload scheduling](/concepts/features/configuration/server-config/workload-scheduling): a kind of access, such as disk reads or CPU time, that is scheduled between the workloads created with [`CREATE WORKLOAD`](/reference/statements/create/workload).

## Syntax {#syntax}

```sql
CREATE [OR REPLACE] RESOURCE [IF NOT EXISTS] name [ON CLUSTER cluster] (access_mode [, ...])
```

`OR REPLACE`, `IF NOT EXISTS` and `ON CLUSTER` work as in [`CREATE WORKLOAD`](/reference/statements/create/workload#syntax).

## Access modes {#access-modes}

| Access mode | Scheduled access | Introduced in |
|---|---|---|
| `READ DISK disk_name`, `WRITE DISK disk_name` | Reads from or writes to the disk `disk_name`, see [Resources](/concepts/features/configuration/server-config/workload-scheduling#resources) | v24.11.0 |
| `READ ANY DISK`, `WRITE ANY DISK` | Reads from or writes to any disk | v24.11.0 |
| `MASTER THREAD`, `WORKER THREAD` | CPU, see [CPU scheduling](/concepts/features/configuration/server-config/workload-scheduling#cpu_scheduling) | v25.4.0 |
| `QUERY` | Query slots, see [Query slot scheduling](/concepts/features/configuration/server-config/workload-scheduling#query_scheduling) | v25.6.0 |
| `MEMORY RESERVATION` | Memory, see [Memory reservations](/concepts/features/configuration/server-config/workload-scheduling#memory-reservations) | v26.7.0 |

The access modes of one resource must be of one kind: disk, CPU threads, `QUERY` or `MEMORY RESERVATION`. Each of `MASTER THREAD`, `WORKER THREAD`, `QUERY` and `MEMORY RESERVATION` belongs to at most one resource. `CREATE OR REPLACE RESOURCE` cannot change the kind of a resource: drop it and create it again.

<Note>
`MEMORY RESERVATION` is experimental: its SQL surface and behavior may change in future releases.
</Note>

## Storage {#storage}

Resources are stored like workloads, see [Storage](/reference/statements/create/workload#storage). The [`system.resources`](/reference/system-tables/resources) table lists the resources.

## Privileges {#privileges}

`CREATE RESOURCE` requires the `CREATE RESOURCE` privilege. `CREATE OR REPLACE RESOURCE` also requires the `DROP RESOURCE` privilege.

## Example {#example}

```sql title="Query"
CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD);
CREATE RESOURCE local_io (READ DISK default, WRITE DISK default);
SELECT name, read_disks, write_disks, unit, create_query FROM system.resources ORDER BY name;
```

```text title="Response"
┌─name─────┬─read_disks──┬─write_disks─┬─unit──────────┬─create_query─────────────────────────────────────────────────────┐
│ cpu      │ []          │ []          │ CPUNanosecond │ CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)               │
│ local_io │ ['default'] │ ['default'] │ IOByte        │ CREATE RESOURCE local_io (READ DISK default, WRITE DISK default) │
└──────────┴─────────────┴─────────────┴───────────────┴──────────────────────────────────────────────────────────────────┘
```

## Related content {#related-content}

- [Workload scheduling](/concepts/features/configuration/server-config/workload-scheduling)
- [`DROP RESOURCE`](/reference/statements/drop#drop-resource)
- [`system.resources`](/reference/system-tables/resources)
)DOCS_MD",
        .syntax = R"(
CREATE [OR REPLACE] RESOURCE [IF NOT EXISTS] name [ON CLUSTER cluster] (access_mode [, ...])
)",
        .parent = "CREATE",
        .related = {"CREATE WORKLOAD", "DROP"},
    };

    return documentation;
}

}
