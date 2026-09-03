#include <Access/IAccessStorage.h>
#include <Parsers/Access/ParserCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Access/Common/RowPolicyDefs.h>
#include <base/range.h>
#include <boost/container/flat_set.hpp>
#include <base/insertAtEnd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_short_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::RENAME_TO}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_short_name);
        });
    }

    bool parseAsRestrictiveOrPermissive(IParserBase::Pos & pos, Expected & expected, bool & is_restrictive)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::AS}.ignore(pos, expected))
                return false;

            if (ParserKeyword{Keyword::RESTRICTIVE}.ignore(pos, expected))
            {
                is_restrictive = true;
                return true;
            }

            if (!ParserKeyword{Keyword::PERMISSIVE}.ignore(pos, expected))
                return false;

            is_restrictive = false;
            return true;
        });
    }

    bool parseFilterExpression(IParserBase::Pos & pos, Expected & expected, ASTPtr & expr)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword(Keyword::NONE).ignore(pos, expected))
            {
                expr = nullptr;
                return true;
            }

            ParserExpression parser;
            ASTPtr x;
            if (!parser.parse(pos, x, expected))
                return false;

            /// This only checks for top-level aliases, nested aliases are always parenthesized so they
            /// do not cause a formatting inconsistency.
            if (!x->tryGetAlias().empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Top-level aliases are not allowed in row policy filter expressions.");

            expr = x;
            return true;
        });
    }


    void addAllCommands(boost::container::flat_set<std::string_view> & commands)
    {
        for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
        {
            std::string_view command = RowPolicyFilterTypeInfo::get(filter_type).command;
            commands.emplace(command);
        }
    }


    bool parseCommands(IParserBase::Pos & pos, Expected & expected,
                       boost::container::flat_set<std::string_view> & commands)
    {
        boost::container::flat_set<std::string_view> res_commands;

        auto parse_command = [&]
        {
            if (ParserKeyword{Keyword::ALL}.ignore(pos, expected))
            {
                addAllCommands(res_commands);
                return true;
            }

            for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
            {
                std::string_view command = RowPolicyFilterTypeInfo::get(filter_type).command;
                if (ParserKeyword::createDeprecated({command.data(), command.size()}).ignore(pos, expected))
                {
                    res_commands.emplace(command);
                    return true;
                }
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_command, false))
            return false;

        commands = std::move(res_commands);
        return true;
    }


    bool parseForClauses(
        IParserBase::Pos & pos, Expected & expected, bool alter, std::vector<std::pair<RowPolicyFilterType, ASTPtr>> & filters)
    {
        std::vector<std::pair<RowPolicyFilterType, ASTPtr>> res_filters;

        auto parse_for_clause = [&]
        {
            boost::container::flat_set<std::string_view> commands;

            if (ParserKeyword{Keyword::FOR}.ignore(pos, expected))
            {
                if (!parseCommands(pos, expected, commands))
                    return false;
            }
            else
                addAllCommands(commands);

            std::optional<ASTPtr> filter;
            std::optional<ASTPtr> check;
            if (ParserKeyword{Keyword::USING}.ignore(pos, expected))
            {
                if (!parseFilterExpression(pos, expected, filter.emplace()))
                    return false;
            }
            if (ParserKeyword{Keyword::WITH_CHECK}.ignore(pos, expected))
            {
                if (!parseFilterExpression(pos, expected, check.emplace()))
                    return false;
            }

            if (!filter && !check)
                return false;

            if (!check && !alter)
                check = filter;

            for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
            {
                const auto & type_info = RowPolicyFilterTypeInfo::get(filter_type);
                if (commands.count(type_info.command))
                {
                    if (type_info.is_check && check)
                        res_filters.emplace_back(filter_type, *check);
                    else if (filter)
                        res_filters.emplace_back(filter_type, *filter);
                }
            }

            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_for_clause, false))
            return false;

        filters = std::move(res_filters);
        return true;
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!ParserKeyword{Keyword::TO}.ignore(pos, expected))
                return false;

            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoles().allowUsers().allowCurrentUser().useIDMode(id_mode);
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateRowPolicyQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_POLICY}.ignore(pos, expected) && !ParserKeyword{Keyword::ATTACH_ROW_POLICY}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_POLICY}.ignore(pos, expected) || ParserKeyword{Keyword::ALTER_ROW_POLICY}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_POLICY}.ignore(pos, expected) && !ParserKeyword{Keyword::CREATE_ROW_POLICY}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{Keyword::IF_NOT_EXISTS}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{Keyword::OR_REPLACE}.ignore(pos, expected))
            or_replace = true;
    }

    ParserRowPolicyNames names_parser;
    names_parser.allowOnCluster();
    ASTPtr names_ast;
    if (!names_parser.parse(pos, names_ast, expected))
        return false;

    auto names = boost::static_pointer_cast<ASTRowPolicyNames>(names_ast);
    String cluster = std::exchange(names->cluster, "");

    String new_short_name;
    std::optional<bool> is_restrictive;
    std::vector<std::pair<RowPolicyFilterType, ASTPtr>> filters;
    String storage_name;

    while (true)
    {
        if (alter && (names->full_names.size() == 1) && new_short_name.empty() && parseRenameTo(pos, expected, new_short_name))
            continue;

        if (!is_restrictive)
        {
            bool new_is_restrictive = false;
            if (parseAsRestrictiveOrPermissive(pos, expected, new_is_restrictive))
            {
                is_restrictive = new_is_restrictive;
                continue;
            }
        }

        std::vector<std::pair<RowPolicyFilterType, ASTPtr>> new_filters;
        if (parseForClauses(pos, expected, alter, new_filters))
        {
            insertAtEnd(filters, std::move(new_filters));
            continue;
        }

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        break;
    }

    boost::intrusive_ptr<ASTRolesOrUsersSet> roles;
    parseToRoles(pos, expected, attach_mode, roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = make_intrusive<ASTCreateRowPolicyQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_short_name = std::move(new_short_name);
    query->is_restrictive = is_restrictive;
    query->filters = std::move(filters);
    query->roles = std::move(roles);
    query->storage_name = std::move(storage_name);

    return true;
}
}

namespace DB
{

void registerStatementRowPolicy(StatementFactory & factory)
{
    factory.registerStatement("CREATE ROW POLICY",
    {
        .description = R"DOCS_MD(
Creates a [row policy](/concepts/features/security/access-rights#row-policy-management), i.e. a filter used to determine which rows a user can read from a table.

<Tip>
Row policies make sense only for users with readonly access. If a user can modify a table or copy partitions between tables, it defeats the restrictions of row policies.
</Tip>

Syntax:

```sql
-- Multiple names on one table target
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [, ...]
    [ON CLUSTER cluster_name]
    ON { [db.]table | db.* }
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]

-- One name on multiple table targets
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name
    [ON CLUSTER cluster_name]
    ON { [db.]table | db.* } [, ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]

-- Mixed packing: each name paired with its own table target
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE]
    policy_name ON { [db.]table | db.* } [, policy_name ON { [db.]table | db.* } ...]
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

`ParserRowPolicyNames` accepts **three** packing forms (not a full Cartesian product):

1. **Multiple names, one target** — `pol1, pol2 ON table1` creates each listed name on that single table (or `db.*`).
2. **One name, multiple targets** — `pol1 ON table1, table2` creates the same short name on each listed target.
3. **Mixed pairs** — `p1 ON t1, p2 ON t2` creates each name only on its paired target.

A multi-name list **cannot** be combined with a multi-table `ON` list in one group: `p1, p2 ON t1, t2` is rejected. After a multi-name group, you also cannot append another comma-separated `name ON target` group in the same statement.

Optional `ON CLUSTER` applies to the whole statement (one cluster name). ClickHouse does **not** accept a different `ON CLUSTER` per policy name packed into a single create — run separate `CREATE ROW POLICY` statements when policies must be created on different clusters.

`CREATE ROW POLICY` requires the [CREATE ROW POLICY](/reference/statements/grant#access-management) privilege on the table the policy is created on. `OR REPLACE` throws away an existing policy of the same name, including which roles it applies to, so it additionally requires the [DROP ROW POLICY](/reference/statements/grant#access-management) privilege on that table. The `DROP ROW POLICY` privilege is required whether or not the policy already exists, so the statement cannot be used to find out which policies exist.

## Multiple names and tables {#multiple-names-and-tables}

Valid:

```sql
-- Several policy names, one table
CREATE ROW POLICY pol1, pol2, pol3 ON table1
    FOR SELECT USING id = 1
    TO accountant;

-- One policy name, several tables
CREATE ROW POLICY IF NOT EXISTS pol1 ON table1, table2, table3
    FOR SELECT USING id = 1
    TO accountant;

-- Mixed packing: different name per table
CREATE ROW POLICY p4 ON db.table, p5 ON db2.table2
    USING a = b;

-- Same policy on several tables, on a cluster
CREATE ROW POLICY IF NOT EXISTS pol1 ON CLUSTER replicated_cluster ON table1, table2
    FOR SELECT USING id = 1
    TO accountant;
```

Invalid:

```sql
-- Multi-name × multi-table in one ON-group (not a Cartesian product)
CREATE ROW POLICY p1, p2 ON t1, t2
    FOR SELECT USING id = 1
    TO accountant;

-- Different clusters per name in one statement
CREATE ROW POLICY pol1 ON CLUSTER cluster1 ON table1, pol2 ON CLUSTER cluster2 ON table2
```

## USING Clause {#using-clause}

Allows specifying a condition to filter rows. A user will see a row if the condition is calculated to non-zero for the row.

## TO Clause {#to-clause}

In the `TO` section you can provide a list of users and roles this policy should work for. For example, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Keyword `ALL` means all the ClickHouse users, including current user. Keyword `ALL EXCEPT` allows excluding some users from the all users list, for example, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

## AS Clause {#as-clause}

It's allowed to have more than one policy enabled on the same table for the same user at one time. So we need a way to combine the conditions from multiple policies.

By default, policies are combined using the boolean `OR` operator. For example, the following policies:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

enable the user `peter` to see rows with either `b=1` or `c=2`.

The `AS` clause specifies how policies should be combined with other policies. Policies can be either permissive or restrictive. By default, policies are permissive, which means they are combined using the boolean `OR` operator.

A policy can be defined as restrictive as an alternative. Restrictive policies are combined using the boolean `AND` operator.

Here is the general formula:

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

For example, the following policies:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

enable the user `peter` to see rows only if both `b=1` AND `c=2`.

Database policies are combined with table policies.

For example, the following policies:

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

enable the user `peter` to see table1 rows only if both `b=1` AND `c=2`, although
any other table in mydb would have only `b=1` policy applied for the user.

## Distributed and remote-backed tables {#distributed-and-remote-backed-tables}

A row policy filters rows where the table data is actually read. A table that delegates reading to remote servers, such as a [Distributed](/reference/engines/table-engines/special/distributed) table or a wrapper over one (for example, a materialized view with a `Distributed` target), only ships the query text to the remote servers and cannot apply the policy filter to the remote read. To keep the filter from being silently dropped, queries to such a table by users the policy applies to are rejected with an `ILLEGAL_PREWHERE` error.

Instead, define the policy on the underlying local tables on each remote server; it is applied there when the shipped query reads them:

```sql
-- Filters reads of local_table on this server, including reads shipped by a Distributed table over it.
CREATE ROW POLICY filter ON mydb.local_table USING a < 1000 TO john;
```

<Warning>
This works while the query is shipped as text, which is the default. With [`serialize_query_plan = 1`](/reference/settings/session-settings/serialize#serialize_query_plan) the initiator ships an already-built read plan instead, and a remote server executing such a plan does not apply its own row policies, so a read of a `Distributed` table over `local_table` returns unfiltered rows. Keep `serialize_query_plan = 0` for users whose row policies must be enforced. See [issue #112891](https://github.com/ClickHouse/ClickHouse/issues/112891).
</Warning>

## ON CLUSTER Clause {#on-cluster-clause}

Allows creating row policies on a cluster, see [Distributed DDL](/reference/statements/distributed-ddl). This is also the convenient way to create the policy on the local tables of every server of the cluster.

## Examples {#examples}

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`
)DOCS_MD",
        .syntax = R"(
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [, ...]
    [ON CLUSTER cluster_name]
    ON { [db.]table | db.* } [, ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
)",
        .parent = "CREATE",
        .related = {"ALTER ROW POLICY", "CREATE MASKING POLICY", "CREATE ROLE", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER ROW POLICY",
    {
        .description = R"DOCS_MD(
Changes row policy.

Syntax:

```sql
-- Rename: exactly one fully qualified policy (one name on one target).
-- RENAME TO may be combined with the same optional alteration clauses as below.
ALTER [ROW] POLICY [IF EXISTS] name
    ON { [database.]table | database.* }
    RENAME TO new_name
    [ON CLUSTER cluster_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]

-- Multiple names on one table target (no RENAME)
ALTER [ROW] POLICY [IF EXISTS] name [, ...]
    [ON CLUSTER cluster_name]
    ON { [database.]table | database.* }
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]

-- One name on multiple table targets (no RENAME)
ALTER [ROW] POLICY [IF EXISTS] name
    [ON CLUSTER cluster_name]
    ON { [database.]table | database.* } [, ...]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]

-- Mixed packing: each name paired with its own table target (no RENAME)
ALTER [ROW] POLICY [IF EXISTS]
    name ON { [database.]table | database.* } [, name ON { [database.]table | database.* } ...]
    [ON CLUSTER cluster_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

`RENAME TO` is only accepted when the statement names **exactly one** policy on **one** table target. Packed multi-name or multi-table lists cannot include `RENAME TO` — rename those policies in separate statements. On that single-policy form, `RENAME TO` can still be combined with other alterations such as `AS`, `USING`, and `TO` in the same statement.

Without `RENAME TO`, packing matches `ParserRowPolicyNames`: multiple names on **one** target, one name on **multiple** targets, or mixed `name ON target` pairs. Multi-name × multi-table (`p1, p2 ON t1, t2`) is **not** accepted. Optional `ON CLUSTER` applies once to the whole statement; run separate `ALTER ROW POLICY` statements when different clusters are required.

Examples:

```sql
-- Single-policy rename
ALTER ROW POLICY p1 ON db.table RENAME TO p1_new;

-- Rename plus other alterations on the same single policy
ALTER POLICY old_name ON db.table RENAME TO new_name USING id > 10;

-- Multiple names, one table
ALTER POLICY p1, p2 ON db.table TO ALL;

-- One name, multiple tables
ALTER POLICY p1 ON db.table, db.table2 USING NONE;

-- Mixed targets without rename
ALTER POLICY p1 ON db.table, p2 ON db2.table2 TO ALL;
```
)DOCS_MD",
        .syntax = R"(
ALTER [ROW] POLICY [IF EXISTS] name [, ...]
    ON { [database.]table | database.* } [, ...]
    [RENAME TO new_name]
    [ON CLUSTER cluster_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
)",
        .parent = "ALTER",
        .related = {"CREATE ROW POLICY", "ALTER", "SHOW"},
    });
}

}
