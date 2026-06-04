#include <algorithm>
#include <unordered_set>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPipeOperators.h>
#include <Parsers/ParserTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED;
extern const int ROW_AND_ROWS_TOGETHER;
}

bool ParserPipeWhere::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ASTPtr condition;
    if (!ParserExpressionWithOptionalAlias(false).parse(pos, condition, expected))
        return false;

    if (query.where())
        condition = makeASTFunction("and", query.where()->clone(), condition);

    query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(condition));
    return true;
}

bool ParserPipeOrderBy::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    auto saved_pos = pos;
    bool is_order_by_all = false;

    ASTPtr order_expression_list;
    ASTPtr interpolate_expression_list;

    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_interpolate(Keyword::INTERPOLATE);

    ParserToken s_open_bracket(TokenType::OpeningRoundBracket);
    ParserToken s_closing_bracket(TokenType::ClosingRoundBracket);

    if (s_all.ignore(pos, expected))
    {
        is_order_by_all = true;

        ParserKeyword s_desc(Keyword::DESC);
        ParserKeyword s_descending(Keyword::DESCENDING);
        ParserKeyword s_asc(Keyword::ASC);
        ParserKeyword s_ascending(Keyword::ASCENDING);
        ParserKeyword s_nulls(Keyword::NULLS);
        ParserKeyword s_last(Keyword::LAST);

        int direction = 1;
        int nulls_direction = 1;
        bool nulls_direction_was_explicitly_specified = false;

        if (s_desc.ignore(pos, expected) || s_descending.ignore(pos, expected))
        {
            direction = -1;
            nulls_direction = -1;
        }
        else
        {
            s_asc.ignore(pos, expected) || s_ascending.ignore(pos, expected);
        }

        if (s_nulls.ignore(pos, expected))
        {
            nulls_direction_was_explicitly_specified = true;
            if (ParserKeyword(Keyword::FIRST).ignore(pos, expected))
                nulls_direction = -direction;
            else if (s_last.ignore(pos, expected))
                ;
            else
                return false;
        }

        if (pos->type == TokenType::Comma)
        {
            pos = saved_pos;
            is_order_by_all = false;
        }
        else
        {
            query.order_by_all = true;

            auto elem = make_intrusive<ASTOrderByElement>();
            elem->direction = direction;
            elem->nulls_direction = nulls_direction;
            elem->nulls_direction_was_explicitly_specified = nulls_direction_was_explicitly_specified;
            elem->children.push_back(make_intrusive<ASTIdentifier>("all"));

            order_expression_list = make_intrusive<ASTExpressionList>();
            order_expression_list->children.push_back(std::move(elem));
        }
    }

    if (!is_order_by_all)
    {
        if (!ParserOrderByExpressionList().parse(pos, order_expression_list, expected))
            return false;

        if (std::any_of(
                order_expression_list->children.begin(),
                order_expression_list->children.end(),
                [](auto & child) { return child->template as<ASTOrderByElement>()->with_fill; }))
        {
            if (s_interpolate.ignore(pos, expected))
            {
                if (s_open_bracket.ignore(pos, expected))
                {
                    if (!ParserInterpolateExpressionList().parse(pos, interpolate_expression_list, expected))
                        return false;
                    if (!s_closing_bracket.ignore(pos, expected))
                        return false;
                }
                else
                {
                    interpolate_expression_list = make_intrusive<ASTExpressionList>();
                }
            }
        }
    }

    query.setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    query.setExpression(ASTSelectQuery::Expression::INTERPOLATE, std::move(interpolate_expression_list));
    return true;
}

bool ParserPipeLimit::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserNotEmptyExpressionList exp_list(false);
    ParserToken s_comma(TokenType::Comma);
    ParserKeyword s_offset(Keyword::OFFSET);
    ParserKeyword s_by(Keyword::BY);
    ParserKeyword s_with_ties(Keyword::WITH_TIES);
    ParserKeyword s_all(Keyword::ALL);

    ASTPtr limit_length;
    ASTPtr limit_by_length;
    ASTPtr limit_offset;
    ASTPtr limit_by_offset;
    ASTPtr limit_by_expression_list;

    if (!exp_elem.parse(pos, limit_length, expected))
        return false;

    bool limit_with_ties_occurred = false;

    if (s_comma.ignore(pos, expected))
    {
        limit_offset = limit_length;
        if (!exp_elem.parse(pos, limit_length, expected))
            return false;

        if (s_with_ties.ignore(pos, expected))
        {
            limit_with_ties_occurred = true;
            query.limit_with_ties = true;
        }
    }
    else if (s_offset.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, limit_offset, expected))
            return false;

        if (s_with_ties.ignore(pos, expected))
        {
            limit_with_ties_occurred = true;
            query.limit_with_ties = true;
        }
    }
    else if (s_with_ties.ignore(pos, expected))
    {
        limit_with_ties_occurred = true;
        query.limit_with_ties = true;
    }

    if (s_by.ignore(pos, expected))
    {
        if (limit_with_ties_occurred)
            throw Exception(ErrorCodes::LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED, "Can not use WITH TIES alongside LIMIT BY/DISTINCT ON");


        limit_by_length = limit_length;
        limit_by_offset = limit_offset;
        limit_length = nullptr;
        limit_offset = nullptr;

        if (s_all.ignore(pos, expected))
        {
            query.limit_by_all = true;
            limit_by_expression_list = make_intrusive<ASTExpressionList>();
        }
        else
        {
            if (!exp_list.parse(pos, limit_by_expression_list, expected))
                return false;
        }
    }

    query.setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, std::move(limit_by_offset));
    query.setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, std::move(limit_by_length));
    query.setExpression(ASTSelectQuery::Expression::LIMIT_BY, std::move(limit_by_expression_list));
    query.setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(limit_offset));
    query.setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));
    return true;
}

bool ParserPipeOffset::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserKeyword s_row(Keyword::ROW);
    ParserKeyword s_rows(Keyword::ROWS);

    ASTPtr limit_offset;

    if (!exp_elem.parse(pos, limit_offset, expected))
        return false;

    bool has_row = false;

    if (s_row.ignore(pos, expected))
        has_row = true;

    if (s_rows.ignore(pos, expected))
    {
        if (has_row)
            throw Exception(ErrorCodes::ROW_AND_ROWS_TOGETHER, "Can not use ROW and ROWS together");
    }

    query.setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(limit_offset));
    return true;
}

bool ParserPipeJoin::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    auto tables_ast = query.tables();
    if (!tables_ast)
        return false;

    auto * tables = tables_ast->as<ASTTablesInSelectQuery>();
    if (!tables)
        return false;

    ASTPtr join_element;
    if (!ParserTablesInSelectQueryElement(false).parse(pos, join_element, expected))
        return false;

    auto * element = join_element->as<ASTTablesInSelectQueryElement>();
    if (!element || !element->table_join)
        return false;

    tables->children.emplace_back(std::move(join_element));
    return true;
}

bool ParserPipeAggregate::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ParserKeyword s_group_by(Keyword::GROUP_BY);
    ParserKeyword s_with(Keyword::WITH);
    ParserKeyword s_rollup(Keyword::ROLLUP);
    ParserKeyword s_cube(Keyword::CUBE);
    ParserKeyword s_grouping_sets(Keyword::GROUPING_SETS);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_totals(Keyword::TOTALS);

    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    ParserNotEmptyExpressionList exp_list(false);
    ParserGroupingSetsExpressionList grouping_sets_list;

    ASTPtr select_expression_list;
    ASTPtr group_expression_list;

    if (!ParserNotEmptyExpressionList(true, true).parse(pos, select_expression_list, expected))
        return false;

    if (s_group_by.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
            query.group_by_with_rollup = true;
        else if (s_cube.ignore(pos, expected))
            query.group_by_with_cube = true;
        else if (s_grouping_sets.ignore(pos, expected))
            query.group_by_with_grouping_sets = true;
        else if (s_all.ignore(pos, expected))
            query.group_by_all = true;

        if ((query.group_by_with_rollup || query.group_by_with_cube || query.group_by_with_grouping_sets)
            && !open_bracket.ignore(pos, expected))
            return false;

        if (query.group_by_with_grouping_sets)
        {
            if (!grouping_sets_list.parse(pos, group_expression_list, expected))
                return false;
        }
        else if (!query.group_by_all)
        {
            if (!exp_list.parse(pos, group_expression_list, expected))
                return false;
        }

        if ((query.group_by_with_rollup || query.group_by_with_cube || query.group_by_with_grouping_sets)
            && !close_bracket.ignore(pos, expected))
            return false;
    }

    if (s_with.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
            query.group_by_with_rollup = true;
        else if (s_cube.ignore(pos, expected))
            query.group_by_with_cube = true;
        else if (s_totals.ignore(pos, expected))
            query.group_by_with_totals = true;
        else
            return false;
    }

    if (s_with.ignore(pos, expected))
    {
        if (query.group_by_with_totals || !s_totals.ignore(pos, expected))
            return false;

        query.group_by_with_totals = true;
    }

    if (group_expression_list)
    {
        // The output of AGGREGATE is the grouping columns followed by the aggregate
        // expressions. A grouping expression is only prepended when its result is not
        // already present in the user-provided projection, so that
        //   AGGREGATE number % 2 AS k, count() AS c GROUP BY number % 2
        // produces `number % 2 AS k, count() AS c` instead of duplicating `number % 2`.
        // A projection expression is matched both by its output name (the alias, e.g.
        // `GROUP BY k`) and by its alias-less form (the expression, e.g. `GROUP BY number % 2`).
        std::unordered_set<String> projected;
        for (const auto & expr : select_expression_list->children)
        {
            projected.insert(expr->getAliasOrColumnName());
            projected.insert(expr->getColumnNameWithoutAlias());
        }

        // new vector created in order to preserve order SELECT <group_cols>, <agg_expr>
        ASTs prepended_grouping_cols;
        auto prepend_if_absent = [&](const ASTPtr & expr)
        {
            if (projected.contains(expr->getAliasOrColumnName()) || projected.contains(expr->getColumnNameWithoutAlias()))
                return;

            prepended_grouping_cols.push_back(expr->clone());
            // Record the prepended column so a repeated grouping expression is not prepended twice.
            projected.insert(expr->getAliasOrColumnName());
            projected.insert(expr->getColumnNameWithoutAlias());
        };

        if (query.group_by_with_grouping_sets)
        {
            for (const auto & grouping_set : group_expression_list->children)
                for (const auto & expr : grouping_set->children)
                    prepend_if_absent(expr);
        }
        else
        {
            for (const auto & expr : group_expression_list->children)
                prepend_if_absent(expr);
        }

        for (const auto & expr : select_expression_list->children)
        {
            prepended_grouping_cols.push_back(expr->clone());
        }

        select_expression_list->children = std::move(prepended_grouping_cols);
    }

    query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    query.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    return true;
}

}
