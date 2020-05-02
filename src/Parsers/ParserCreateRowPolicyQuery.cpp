#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Access/RowPolicy.h>
#include <Parsers/ParserExtendedRoleSet.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace ErrorCodes
{
}


namespace
{
    using ConditionType = RowPolicy::ConditionType;

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

    bool parseConditions(IParserBase::Pos & pos, Expected & expected, bool alter, std::vector<std::pair<ConditionType, ASTPtr>> & conditions)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            static constexpr char select_op[] = "SELECT";
            static constexpr char insert_op[] = "INSERT";
            static constexpr char update_op[] = "UPDATE";
            static constexpr char delete_op[] = "DELETE";
            std::vector<const char *> ops;

            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                do
                {
                    if (ParserKeyword{"SELECT"}.ignore(pos, expected))
                        ops.push_back(select_op);
#if 0 /// INSERT, UPDATE, DELETE are not supported yet
                    else if (ParserKeyword{"INSERT"}.ignore(pos, expected))
                        ops.push_back(insert_op);
                    else if (ParserKeyword{"UPDATE"}.ignore(pos, expected))
                        ops.push_back(update_op);
                    else if (ParserKeyword{"DELETE"}.ignore(pos, expected))
                        ops.push_back(delete_op);
                    else if (ParserKeyword{"ALL"}.ignore(pos, expected))
                    {
                    }
#endif
                    else
                        return false;
                }
                while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            }

            if (ops.empty())
            {
                ops.push_back(select_op);
#if 0 /// INSERT, UPDATE, DELETE are not supported yet
                ops.push_back(insert_op);
                ops.push_back(update_op);
                ops.push_back(delete_op);
#endif
            }

            std::optional<ASTPtr> filter;
            std::optional<ASTPtr> check;
            bool keyword_using = false, keyword_with_check = false;
            if (ParserKeyword{"USING"}.ignore(pos, expected))
            {
                keyword_using = true;
                if (!parseConditionalExpression(pos, expected, filter))
                    return false;
            }
#if 0 /// INSERT, UPDATE, DELETE are not supported yet
            if (ParserKeyword{"WITH CHECK"}.ignore(pos, expected))
            {
                keyword_with_check = true;
                if (!parseConditionalExpression(pos, expected, check))
                    return false;
            }
#endif
            if (!keyword_using && !keyword_with_check)
                return false;

            if (filter && !check && !alter)
                check = filter;

            auto set_condition = [&](ConditionType index, const ASTPtr & condition)
            {
                auto it = std::find_if(conditions.begin(), conditions.end(), [index](const std::pair<ConditionType, ASTPtr> & element)
                {
                    return element.first == index;
                });
                if (it == conditions.end())
                    it = conditions.insert(conditions.end(), std::pair<ConditionType, ASTPtr>{index, nullptr});
                it->second = condition;
            };

            for (const auto & op : ops)
            {
                if ((op == select_op) && filter)
                    set_condition(RowPolicy::SELECT_FILTER, *filter);
                else if ((op == insert_op) && check)
                    set_condition(RowPolicy::INSERT_CHECK, *check);
                else if (op == update_op)
                {
                    if (filter)
                        set_condition(RowPolicy::UPDATE_FILTER, *filter);
                    if (check)
                        set_condition(RowPolicy::UPDATE_CHECK, *check);
                }
                else if ((op == delete_op) && filter)
                    set_condition(RowPolicy::DELETE_FILTER, *filter);
                else
                    __builtin_unreachable();
            }

            return true;
        });
    }

    bool parseMultipleConditions(IParserBase::Pos & pos, Expected & expected, bool alter, std::vector<std::pair<ConditionType, ASTPtr>> & conditions)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<std::pair<ConditionType, ASTPtr>> res_conditions;
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

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTExtendedRoleSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (roles || !ParserKeyword{"TO"}.ignore(pos, expected)
                || !ParserExtendedRoleSet{}.useIDMode(id_mode).parse(pos, ast, expected))
                return false;

            roles = std::static_pointer_cast<ASTExtendedRoleSet>(ast);
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

    RowPolicy::NameParts name_parts;
    String & database = name_parts.database;
    String & table_name = name_parts.table_name;
    String & short_name = name_parts.short_name;
    if (!parseIdentifierOrStringLiteral(pos, expected, short_name) || !ParserKeyword{"ON"}.ignore(pos, expected)
        || !parseDatabaseAndTableName(pos, expected, database, table_name))
        return false;

    String new_short_name;
    std::optional<bool> is_restrictive;
    std::vector<std::pair<ConditionType, ASTPtr>> conditions;
    String cluster;

    while (true)
    {
        if (alter && new_short_name.empty() && parseRenameTo(pos, expected, new_short_name))
            continue;

        if (!is_restrictive && parseAsRestrictiveOrPermissive(pos, expected, is_restrictive))
            continue;

        if (parseMultipleConditions(pos, expected, alter, conditions))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        break;
    }

    std::shared_ptr<ASTExtendedRoleSet> roles;
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
    query->name_parts = std::move(name_parts);
    query->new_short_name = std::move(new_short_name);
    query->is_restrictive = is_restrictive;
    query->conditions = std::move(conditions);
    query->roles = std::move(roles);

    return true;
}
}
