#include <Parsers/Access/ParserCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Access/IAccessStorage.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::RENAME_TO}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseUpdateAssignments(IParserBase::Pos & pos, Expected & expected, ASTPtr & assignments)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::UPDATE}.ignore(pos, expected))
                return false;

            ParserList parser_assignment_list(
                std::make_unique<ParserAssignment>(),
                std::make_unique<ParserToken>(TokenType::Comma),
                /* allow_empty = */ false);

            if (!parser_assignment_list.parse(pos, assignments, expected))
                return false;

            return true;
        });
    }

    bool parseWhereCondition(IParserBase::Pos & pos, Expected & expected, ASTPtr & condition)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::WHERE}.ignore(pos, expected))
                return false;

            ParserExpression parser;
            if (!parser.parse(pos, condition, expected))
                return false;

            return true;
        });
    }

    bool parsePriority(IParserBase::Pos & pos, Expected & expected, Int64 & priority)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::PRIORITY}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserNumber parser;
            if (!parser.parse(pos, ast, expected))
                return false;

            const auto * literal = ast->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::UInt64)
                return false;

            priority = literal->value.safeGet<UInt64>();
            return true;
        });
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


bool ParserCreateMaskingPolicy::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;

    if (ParserKeyword{Keyword::ATTACH_MASKING_POLICY}.ignore(pos, expected))
        attach_mode = true;
    else if (ParserKeyword{Keyword::ALTER_MASKING_POLICY}.ignore(pos, expected))
        alter = true;
    else if (!ParserKeyword{Keyword::CREATE_MASKING_POLICY}.ignore(pos, expected))
        return false;

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

    String name;
    if (!parseIdentifierOrStringLiteral(pos, expected, name))
        return false;

    if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
        return false;

    String database;
    String table_name;
    if (!parseDatabaseAndTableName(pos, expected, database, table_name))
        return false;

    String cluster;

    String new_name;
    ASTPtr update_assignments;
    ASTPtr where_condition;
    boost::intrusive_ptr<ASTRolesOrUsersSet> roles;
    Int64 priority = 0;
    bool has_priority = false;
    String storage_name;

    while (true)
    {
        if (alter && new_name.empty() && parseRenameTo(pos, expected, new_name))
            continue;

        if (!update_assignments && parseUpdateAssignments(pos, expected, update_assignments))
            continue;

        if (!where_condition && parseWhereCondition(pos, expected, where_condition))
            continue;

        if (!roles && parseToRoles(pos, expected, attach_mode, roles))
            continue;

        if (!has_priority)
        {
            Int64 new_priority = 0;
            if (parsePriority(pos, expected, new_priority))
            {
                priority = new_priority;
                has_priority = true;
                continue;
            }
        }

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        break;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (!roles && !alter)
    {
        roles = make_intrusive<ASTRolesOrUsersSet>();
        roles->all = true;
    }

    if (!update_assignments && !alter && !attach_mode)
        return false;

    auto query = make_intrusive<ASTCreateMaskingPolicyQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->name = std::move(name);
    query->database = std::move(database);
    query->table_name = std::move(table_name);
    query->new_name = std::move(new_name);
    query->update_assignments = std::move(update_assignments);
    query->where_condition = std::move(where_condition);
    query->roles = std::move(roles);
    query->priority = priority;
    query->storage_name = std::move(storage_name);

    return true;
}
}

namespace DB
{

void registerStatementMaskingPolicy(StatementFactory & factory)
{
    factory.registerStatement("CREATE MASKING POLICY",
    {
        .description = R"(
Creates a masking policy, which dynamically transforms or masks the values of columns for specific users or roles when
they query a table. Masking policies provide column-level data security by transforming sensitive data at query time,
without modifying the stored data.

**Examples**

**Mask the values of a column for a role**

```sql title="Query"
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```
)",
        .syntax = R"(
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
)",
        .parent = "CREATE",
        .related = {"ALTER MASKING POLICY", "CREATE ROW POLICY", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER MASKING POLICY",
    {
        .description = R"(
Modifies an existing masking policy. All clauses are optional; only the specified clauses are changed.

**Examples**

**Change the roles a masking policy applies to**

```sql title="Query"
ALTER MASKING POLICY mask_high_salaries ON employees TO analyst, accountant;
```
)",
        .syntax = R"(
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
)",
        .parent = "ALTER",
        .related = {"CREATE MASKING POLICY", "ALTER", "SHOW"},
    });
}

}
