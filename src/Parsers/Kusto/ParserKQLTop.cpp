#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLTop.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Poco/String.h>

#include <fmt/format.h>

namespace DB
{

bool ParserKQLTop::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// top N by column [asc|desc]
    /// e.g., top 3 by LineCount

    if (!isValidKQLPos(pos))
        return false;

    /// Get N (limit) — resolve let bindings
    String limit_str(pos->begin, pos->end);
    auto & bindings = kqlLetBindings();
    if (auto it = bindings.find(limit_str); it != bindings.end())
        limit_str = it->second;
    ++pos;

    /// Expect "by"
    if (!isValidKQLPos(pos))
        return false;

    /// KQL keywords are case-insensitive, so accept `BY`/`By`/etc.
    const String by_token = Poco::toLower(String(pos->begin, pos->end));
    if (by_token != "by")
        return false;
    ++pos;

    /// Get the sort expression (rest of the pipe)
    auto sort_expr = getExprFromPipe(pos);
    if (sort_expr.empty())
        return false;

    /// Parse order by
    ParserOrderByExpressionList order_list;
    ASTPtr order_expression_list;

    Tokens sort_tokens(sort_expr.data(), sort_expr.data() + sort_expr.size(), 0, true);
    IParser::Pos sort_pos(sort_tokens, pos.max_depth, pos.max_backtracks);

    if (!order_list.parse(sort_pos, order_expression_list, expected))
        return false;

    /// Default to desc per key when its direction was not explicitly specified.
    /// We re-scan the source text split by top-level commas, because `ASTOrderByElement::direction`
    /// defaults to 1 (ASC) when omitted, so we cannot distinguish "user wrote asc" from "no direction"
    /// by inspecting the parsed AST alone.
    {
        std::vector<bool> element_has_explicit_dir;
        element_has_explicit_dir.reserve(order_expression_list->children.size());

        Tokens split_tokens(sort_expr.data(), sort_expr.data() + sort_expr.size(), 0, true);
        IParser::Pos split_pos(split_tokens, pos.max_depth, pos.max_backtracks);
        int depth = 0;
        bool seen_asc_desc = false;
        while (isValidKQLPos(split_pos))
        {
            if (split_pos->type == TokenType::OpeningRoundBracket || split_pos->type == TokenType::OpeningSquareBracket)
                ++depth;
            else if (split_pos->type == TokenType::ClosingRoundBracket || split_pos->type == TokenType::ClosingSquareBracket)
                --depth;
            else if (depth == 0 && split_pos->type == TokenType::Comma)
            {
                element_has_explicit_dir.push_back(seen_asc_desc);
                seen_asc_desc = false;
                ++split_pos;
                continue;
            }
            else if (depth == 0 && split_pos->type == TokenType::BareWord)
            {
                /// KQL keywords are case-insensitive, so accept `ASC`/`Desc`/etc.
                const String tok = Poco::toLower(String(split_pos->begin, split_pos->end));
                if (tok == "asc" || tok == "desc")
                    seen_asc_desc = true;
            }
            ++split_pos;
        }
        element_has_explicit_dir.push_back(seen_asc_desc);

        /// Apply KQL default (desc) only to elements without explicit direction.
        const auto & children = order_expression_list->children;
        for (size_t i = 0; i < children.size(); ++i)
        {
            if (i < element_has_explicit_dir.size() && element_has_explicit_dir[i])
                continue;
            auto * order_expr = children[i]->as<ASTOrderByElement>();
            order_expr->direction = -1;
            if (!order_expr->nulls_direction_was_explicitly_specified)
                order_expr->nulls_direction = -1;
        }
    }

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));

    /// Set limit
    ASTPtr limit_length;
    Tokens limit_tokens(limit_str.data(), limit_str.data() + limit_str.size(), 0, true);
    IParser::Pos limit_pos(limit_tokens, pos.max_depth, pos.max_backtracks);

    if (!ParserExpressionWithOptionalAlias(false).parse(limit_pos, limit_length, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));

    return true;
}

}
