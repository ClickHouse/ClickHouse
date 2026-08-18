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
        .description = R"(
Creates a row policy, i.e. a filter which determines which rows a user can read from a table. Row policies only make
sense for users with read-only access: a user who can modify a table or copy partitions between tables can defeat the
restrictions of a row policy.

**Examples**

**Restrict the visible rows of a table**

```sql title="Query"
CREATE ROW POLICY pol1 ON table1
    FOR SELECT USING id = 1
    TO accountant;
```
)",
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
        .description = R"(
Changes a row policy: renames it and changes its condition, its kind (permissive or restrictive) and the roles and
users it applies to.

**Examples**

**Rename a row policy**

```sql title="Query"
ALTER ROW POLICY p1 ON db.table RENAME TO p1_new;
```
)",
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
