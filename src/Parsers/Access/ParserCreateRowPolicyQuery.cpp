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
#include <Access/Common/RowPolicyDefs.h>
#include <base/range.h>
#include <boost/container/flat_set.hpp>
#include <base/insertAtEnd.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_short_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_short_name);
        });
    }

    bool parseKind(IParserBase::Pos & pos, Expected & expected, RowPolicyKind & kind)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ParserKeyword{"AS"}.ignore(pos, expected);

            for (auto current_kind : collections::range(RowPolicyKind::MAX))
            {
                const std::string_view & kind_name = RowPolicyKindInfo::get(current_kind).name;
                if (ParserKeyword{kind_name.data()}.ignore(pos, expected))
                {
                    kind = current_kind;
                    return true;
                }
            }

            return false;
        });
    }

    bool parseFilterExpression(IParserBase::Pos & pos, Expected & expected, ASTPtr & expr)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword("NONE").ignore(pos, expected))
            {
                expr = nullptr;
                return true;
            }

            ParserExpression parser;
            ASTPtr x;
            if (!parser.parse(pos, x, expected))
                return false;

            expr = x;
            return true;
        });
    }


    void addAllCommands(boost::container::flat_set<std::string_view> & commands)
    {
        for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
        {
            const std::string_view & command = RowPolicyFilterTypeInfo::get(filter_type).command;
            commands.emplace(command);
        }
    }


    bool parseCommands(IParserBase::Pos & pos, Expected & expected,
                       boost::container::flat_set<std::string_view> & commands)
    {
        boost::container::flat_set<std::string_view> res_commands;

        auto parse_command = [&]
        {
            if (ParserKeyword{"ALL"}.ignore(pos, expected))
            {
                addAllCommands(res_commands);
                return true;
            }

            for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
            {
                const std::string_view & command = RowPolicyFilterTypeInfo::get(filter_type).command;
                if (ParserKeyword{command.data()}.ignore(pos, expected))
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

            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                if (!parseCommands(pos, expected, commands))
                    return false;
            }
            else
                addAllCommands(commands);

            std::optional<ASTPtr> filter;
            std::optional<ASTPtr> check;
            if (ParserKeyword{"USING"}.ignore(pos, expected))
            {
                if (!parseFilterExpression(pos, expected, filter.emplace()))
                    return false;
            }
            if (ParserKeyword{"WITH CHECK"}.ignore(pos, expected))
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

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!ParserKeyword{"TO"}.ignore(pos, expected))
                return false;

            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoles().allowUsers().allowCurrentUser().useIDMode(id_mode);
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = std::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateRowPolicyQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH POLICY"}.ignore(pos, expected) && !ParserKeyword{"ATTACH ROW POLICY"}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER POLICY"}.ignore(pos, expected) || ParserKeyword{"ALTER ROW POLICY"}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{"CREATE POLICY"}.ignore(pos, expected) && !ParserKeyword{"CREATE ROW POLICY"}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected))
            or_replace = true;
    }

    ParserRowPolicyNames names_parser;
    names_parser.allowOnCluster();
    ASTPtr names_ast;
    if (!names_parser.parse(pos, names_ast, expected))
        return false;

    auto names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(names_ast);
    String cluster = std::exchange(names->cluster, "");

    String new_short_name;
    std::vector<std::pair<RowPolicyFilterType, ASTPtr>> filters;
    std::optional<RowPolicyKind> kind;
    std::shared_ptr<ASTRolesOrUsersSet> to_roles;

    while (true)
    {
        if (alter && (names->full_names.size() == 1) && new_short_name.empty() && parseRenameTo(pos, expected, new_short_name))
            continue;

        std::vector<std::pair<RowPolicyFilterType, ASTPtr>> new_filters;
        if (parseForClauses(pos, expected, alter, new_filters))
        {
            insertAtEnd(filters, std::move(new_filters));
            continue;
        }

        if (!kind)
        {
            RowPolicyKind new_kind;
            if (parseKind(pos, expected, new_kind))
            {
                kind = new_kind;
                continue;
            }
        }

        if (!to_roles && parseToRoles(pos, expected, attach_mode, to_roles))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        break;
    }

    auto query = std::make_shared<ASTCreateRowPolicyQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_short_name = std::move(new_short_name);
    query->filters = std::move(filters);
    query->kind = kind;
    query->to_roles = std::move(to_roles);

    return true;
}
}
