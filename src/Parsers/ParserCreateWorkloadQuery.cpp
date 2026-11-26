#include <Parsers/ParserCreateWorkloadQuery.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/SettingsChanges.h>

namespace DB
{

namespace
{

bool parseWorkloadSetting(
    ASTCreateWorkloadQuery::SettingChange & change, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserLiteral value_p;
    ParserToken s_eq(TokenType::Equals);
    ParserIdentifier resource_name_p;

    ASTPtr name_node;
    ASTPtr value_node;
    ASTPtr resource_name_node;

    String name;
    String resource_name;

    if (!name_p.parse(pos, name_node, expected))
        return false;
    tryGetIdentifierNameInto(name_node, name);

    if (!s_eq.ignore(pos, expected))
        return false;

    if (!value_p.parse(pos, value_node, expected))
        return false;

    if (ParserKeyword(Keyword::FOR).ignore(pos, expected))
    {
        if (!resource_name_p.parse(pos, resource_name_node, expected))
            return false;
        tryGetIdentifierNameInto(resource_name_node, resource_name);
    }

    change.name = std::move(name);
    change.value = value_node->as<ASTLiteral &>().value;
    change.resource = std::move(resource_name);

    return true;
}

bool parseSettings(IParser::Pos & pos, Expected & expected, ASTCreateWorkloadQuery::SettingsChanges & changes)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (!ParserKeyword(Keyword::SETTINGS).ignore(pos, expected))
            return false;

        ASTCreateWorkloadQuery::SettingsChanges res_changes;

        auto parse_setting = [&]
        {
            ASTCreateWorkloadQuery::SettingChange change;
            if (!parseWorkloadSetting(change, pos, expected))
                return false;
            res_changes.push_back(std::move(change));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_setting, false))
            return false;

        changes = std::move(res_changes);
        return true;
    });
}

}

bool ParserCreateWorkloadQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_workload(Keyword::WORKLOAD);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserIdentifier workload_name_p;
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_in(Keyword::IN);

    ASTPtr workload_name;
    ASTPtr workload_parent;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_workload.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!workload_name_p.parse(pos, workload_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (s_in.ignore(pos, expected))
    {
        if (!workload_name_p.parse(pos, workload_parent, expected))
            return false;
    }

    ASTCreateWorkloadQuery::SettingsChanges changes;
    parseSettings(pos, expected, changes);

    auto create_workload_query = make_intrusive<ASTCreateWorkloadQuery>();
    node = create_workload_query;

    create_workload_query->workload_name = workload_name;
    create_workload_query->children.push_back(workload_name);

    if (workload_parent)
    {
        create_workload_query->workload_parent = workload_parent;
        create_workload_query->children.push_back(workload_parent);
    }

    create_workload_query->or_replace = or_replace;
    create_workload_query->if_not_exists = if_not_exists;
    create_workload_query->cluster = std::move(cluster_str);
    create_workload_query->changes = std::move(changes);


    return true;
}

std::map<String, Documentation> ParserCreateWorkloadQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["CREATE WORKLOAD"] =
    {
        .description = R"DOCS_MD(
Introduced in: v24.11.0

Creates a workload: a node of the hierarchy that [workload scheduling](/concepts/features/configuration/server-config/workload-scheduling) uses to share resources between workloads. The resources to schedule are created with [`CREATE RESOURCE`](/reference/statements/create/resource).

## Syntax {#syntax}

```sql
CREATE [OR REPLACE] WORKLOAD [IF NOT EXISTS] name [ON CLUSTER cluster] [IN parent_name] [SETTINGS setting = value [FOR resource_name] [, ...]]
```

- `OR REPLACE` - replaces the definition of an existing workload. It cannot be combined with `IF NOT EXISTS`.
- `IF NOT EXISTS` - if a workload or a resource named `name` exists, the query does nothing.
- `IN parent_name` - creates the workload as a child of the workload `parent_name`. A workload without `IN` is a root workload. A cycle of workloads is rejected.
- `SETTINGS` - scheduling settings of the workload, for example `weight`, `priority`, `max_concurrent_threads` or `max_bytes_per_second`, described in [Workload hierarchy](/concepts/features/configuration/server-config/workload-scheduling#workloads). A setting with `FOR resource_name` applies only to that resource, otherwise to every resource. An unknown setting is an error.

Workloads and resources share one namespace. A query runs in the workload named by its [`workload`](/reference/settings/session-settings/other#workload) setting, see [Workload markup](/concepts/features/configuration/server-config/workload-scheduling#workload-markup).

<Note>
Memory reservation scheduling is experimental: the `MEMORY RESERVATION` resource and the workload settings `max_memory`, `max_memory_ratio`, `max_memory_before_spill` and `max_memory_to_spill_ratio`. See [Memory reservations](/concepts/features/configuration/server-config/workload-scheduling#memory-reservations).
</Note>

## Storage {#storage}

Workloads are stored on disk in [`workload_path`](/reference/settings/server-settings/settings/workload#workload_path), or in ZooKeeper at [`workload_zookeeper_path`](/reference/settings/server-settings/settings/workload#workload_zookeeper_path) if it is set. With ZooKeeper storage, `ON CLUSTER` is rejected because the definitions are replicated automatically. Workloads [defined in the server configuration](/concepts/features/configuration/server-config/workload-scheduling#config_based_workloads) cannot be replaced or dropped with SQL. The [`system.workloads`](/reference/system-tables/workloads) table lists the workloads.

## Privileges {#privileges}

`CREATE WORKLOAD` requires the `CREATE WORKLOAD` privilege. `CREATE OR REPLACE WORKLOAD` also requires the `DROP WORKLOAD` privilege.

## Example {#example}

```sql title="Query"
CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD);
CREATE WORKLOAD all SETTINGS max_concurrent_threads = 16;
CREATE WORKLOAD production IN all SETTINGS weight = 3;
CREATE WORKLOAD development IN all;
SELECT name, parent, create_query FROM system.workloads ORDER BY name;
```

```text title="Response"
┌─name────────┬─parent─┬─create_query───────────────────────────────────────────────┐
│ all         │        │ CREATE WORKLOAD `all` SETTINGS max_concurrent_threads = 16 │
│ development │ all    │ CREATE WORKLOAD development IN `all`                       │
│ production  │ all    │ CREATE WORKLOAD production IN `all` SETTINGS weight = 3    │
└─────────────┴────────┴────────────────────────────────────────────────────────────┘
```

## Related content {#related-content}

- [Workload scheduling](/concepts/features/configuration/server-config/workload-scheduling)
- [`DROP WORKLOAD`](/reference/statements/drop#drop-workload)
- [`system.workloads`](/reference/system-tables/workloads)
)DOCS_MD",
        .syntax = R"(
CREATE [OR REPLACE] WORKLOAD [IF NOT EXISTS] name [ON CLUSTER cluster] [IN parent_name] [SETTINGS setting = value [FOR resource_name] [, ...]]
)",
        .parent = "CREATE",
        .related = {"CREATE RESOURCE", "DROP"},
    };

    return documentation;
}

}
