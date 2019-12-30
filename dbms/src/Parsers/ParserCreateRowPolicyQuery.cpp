#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Access/RowPolicy.h>
#include <Parsers/ParserRoleList.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


namespace
{
    using ConditionIndex = RowPolicy::ConditionIndex;

    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_policy_name, bool alter)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!new_policy_name.empty() || !alter)
                return false;

            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_policy_name);
        });
    }

    bool parseIsRestrictive(IParserBase::Pos & pos, Expected & expected, std::optional<bool> & is_restrictive)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (is_restrictive)
                return false;

            if (!ParserKeyword{"AS"}.ignore(pos, expected))
                return false;

            if (ParserKeyword{"RESTRICTIVE"}.ignore(pos, expected))
                is_restrictive = true;
            else if (ParserKeyword{"PERMISSIVE"}.ignore(pos, expected))
                is_restrictive = false;
            else
                return false;

            return true;
        });
    }

    bool parseConditionalExpression(IParserBase::Pos & pos, Expected & expected, std::optional<ASTPtr> & expr)
    {
        if (ParserKeyword("NONE").ignore(pos, expected))
        {
            expr = nullptr;
            return true;
        }
        ParserExpression parser;
        ASTPtr x;
        if (parser.parse(pos, x, expected))
        {
            expr = x;
            return true;
        }
        expr.reset();
        return false;
    }

    bool parseConditions(IParserBase::Pos & pos, Expected & expected, std::vector<std::pair<ConditionIndex, ASTPtr>> & conditions, bool alter)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            static constexpr char select_op[] = "SELECT";
            static constexpr char insert_op[] = "INSERT";
            static constexpr char update_op[] = "UPDATE";
            static constexpr char delete_op[] = "DELETE";
            std::vector<const char *> ops;

            bool keyword_for = false;
            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                keyword_for = true;
                do
                {
                    if (ParserKeyword{"SELECT"}.ignore(pos, expected))
                        ops.push_back(select_op);
                    else if (ParserKeyword{"INSERT"}.ignore(pos, expected))
                        ops.push_back(insert_op);
                    else if (ParserKeyword{"UPDATE"}.ignore(pos, expected))
                        ops.push_back(update_op);
                    else if (ParserKeyword{"DELETE"}.ignore(pos, expected))
                        ops.push_back(delete_op);
                    else if (ParserKeyword{"ALL"}.ignore(pos, expected))
                    {
                    }
                    else
                        return false;
                }
                while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            }

            if (ops.empty())
            {
                ops.push_back(select_op);
                ops.push_back(insert_op);
                ops.push_back(update_op);
                ops.push_back(delete_op);
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
            if (ParserKeyword{"WITH CHECK"}.ignore(pos, expected))
            {
                keyword_with_check = true;
                if (!parseConditionalExpression(pos, expected, check))
                    return false;
            }

            if (!keyword_for && !keyword_using && !keyword_with_check)
                return false;

            if (filter && !check && !alter)
                check = filter;

            auto set_condition = [&](ConditionIndex index, const ASTPtr & condition)
            {
                auto it = std::find_if(conditions.begin(), conditions.end(), [index](const std::pair<ConditionIndex, ASTPtr> & element)
                {
                    return element.first == index;
                });
                if (it == conditions.end())
                    it = conditions.insert(conditions.end(), std::pair<ConditionIndex, ASTPtr>{index, nullptr});
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

    bool parseMultipleConditions(IParserBase::Pos & pos, Expected & expected, std::vector<std::pair<ConditionIndex, ASTPtr>> & conditions, bool alter)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            do
            {
                if (!parseConditions(pos, expected, conditions, alter))
                    return false;
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            return true;
        });
    }

    bool parseRoles(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTRoleList> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr node;
            if (roles || !ParserKeyword{"TO"}.ignore(pos, expected) || !ParserRoleList{}.parse(pos, node, expected))
                return false;

            roles = std::static_pointer_cast<ASTRoleList>(node);
            return true;
        });
    }
}


bool ParserCreateRowPolicyQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter;
    if (ParserKeyword{"CREATE POLICY"}.ignore(pos, expected) || ParserKeyword{"CREATE ROW POLICY"}.ignore(pos, expected))
        alter = false;
    else if (ParserKeyword{"ALTER POLICY"}.ignore(pos, expected) || ParserKeyword{"ALTER ROW POLICY"}.ignore(pos, expected))
        alter = true;
    else
        return false;

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

    RowPolicy::FullNameParts name_parts;
    String & database = name_parts.database;
    String & table_name = name_parts.table_name;
    String & policy_name = name_parts.policy_name;
    if (!parseIdentifierOrStringLiteral(pos, expected, policy_name) || !ParserKeyword{"ON"}.ignore(pos, expected)
        || !parseDatabaseAndTableName(pos, expected, database, table_name))
        return false;

    String new_policy_name;
    std::optional<bool> is_restrictive;
    std::vector<std::pair<ConditionIndex, ASTPtr>> conditions;
    std::shared_ptr<ASTRoleList> roles;

    while (parseRenameTo(pos, expected, new_policy_name, alter) || parseIsRestrictive(pos, expected, is_restrictive)
           || parseMultipleConditions(pos, expected, conditions, alter) || parseRoles(pos, expected, roles))
        ;

    auto query = std::make_shared<ASTCreateRowPolicyQuery>();
    node = query;

    query->alter = alter;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->name_parts = std::move(name_parts);
    query->new_policy_name = std::move(new_policy_name);
    query->is_restrictive = is_restrictive;
    query->conditions = std::move(conditions);
    query->roles = std::move(roles);

    return true;
}
}
