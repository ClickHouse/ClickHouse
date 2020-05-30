#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Access/RowPolicy.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/ParserRowPolicyName.h>
#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using ConditionType = RowPolicy::ConditionType;
    using ConditionTypeInfo = RowPolicy::ConditionTypeInfo;
    constexpr auto MAX_CONDITION_TYPE = RowPolicy::MAX_CONDITION_TYPE;


    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_short_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_short_name);
        });
    }

    bool parseAsRestrictiveOrPermissive(IParserBase::Pos & pos, Expected & expected, std::optional<bool> & is_restrictive)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"AS"}.ignore(pos, expected))
                return false;

            if (ParserKeyword{"RESTRICTIVE"}.ignore(pos, expected))
            {
                is_restrictive = true;
                return true;
            }

            if (!ParserKeyword{"PERMISSIVE"}.ignore(pos, expected))
                return false;

            is_restrictive = false;
            return true;
        });
    }

    bool parseConditionalExpression(IParserBase::Pos & pos, Expected & expected, std::optional<ASTPtr> & expr)
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

    bool parseConditions(
        IParserBase::Pos & pos, Expected & expected, bool alter, std::array<std::optional<ASTPtr>, MAX_CONDITION_TYPE> & conditions)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            boost::container::flat_set<std::string_view> commands;

            auto add_all_commands = [&]
            {
                for (auto condition_type : ext::range(MAX_CONDITION_TYPE))
                {
                    const std::string_view & command = ConditionTypeInfo::get(condition_type).command;
                    commands.emplace(command);
                }
            };

            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                do
                {
                    size_t old_size = commands.size();
                    if (ParserKeyword{"ALL"}.ignore(pos, expected))
                    {
                        add_all_commands();
                    }
                    else
                    {
                        for (auto condition_type : ext::range(MAX_CONDITION_TYPE))
                        {
                            const std::string_view & command = ConditionTypeInfo::get(condition_type).command;
                            if (ParserKeyword{command.data()}.ignore(pos, expected))
                            {
                                commands.emplace(command);
                                break;
                            }
                        }
                    }
                    if (commands.size() == old_size)
                        return false;
                }
                while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            }

            std::optional<ASTPtr> filter;
            std::optional<ASTPtr> check;
            if (ParserKeyword{"USING"}.ignore(pos, expected))
            {
                if (!parseConditionalExpression(pos, expected, filter))
                    return false;
            }
            if (ParserKeyword{"WITH CHECK"}.ignore(pos, expected))
            {
                if (!parseConditionalExpression(pos, expected, check))
                    return false;
            }

            if (!filter && !check)
                return false;

            if (commands.empty())
                add_all_commands();

            if (!check && !alter)
                check = filter;

            for (auto condition_type : ext::range(MAX_CONDITION_TYPE))
            {
                const auto & type_info = ConditionTypeInfo::get(condition_type);
                if (commands.count(type_info.command))
                {
                    if (type_info.is_check && check)
                        conditions[condition_type] = check;
                    else if (filter)
                        conditions[condition_type] = filter;
                }
            }

            return true;
        });
    }

    bool parseMultipleConditions(
        IParserBase::Pos & pos, Expected & expected, bool alter, std::array<std::optional<ASTPtr>, MAX_CONDITION_TYPE> & conditions)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::array<std::optional<ASTPtr>, MAX_CONDITION_TYPE> res_conditions;
            do
            {
                if (!parseConditions(pos, expected, alter, res_conditions))
                    return false;
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            conditions = std::move(res_conditions);
            return true;
        });
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (roles || !ParserKeyword{"TO"}.ignore(pos, expected))
                return false;

            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoleNames().allowUserNames().allowCurrentUser().useIDMode(id_mode);
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
    std::optional<bool> is_restrictive;
    std::array<std::optional<ASTPtr>, MAX_CONDITION_TYPE> conditions;

    while (true)
    {
        if (alter && new_short_name.empty() && (names->name_parts.size() == 1) && parseRenameTo(pos, expected, new_short_name))
            continue;

        if (!is_restrictive && parseAsRestrictiveOrPermissive(pos, expected, is_restrictive))
            continue;

        if (parseMultipleConditions(pos, expected, alter, conditions))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        break;
    }

    std::shared_ptr<ASTRolesOrUsersSet> roles;
    parseToRoles(pos, expected, attach_mode, roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

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
    query->is_restrictive = is_restrictive;
    query->conditions = std::move(conditions);
    query->roles = std::move(roles);

    return true;
}
}
