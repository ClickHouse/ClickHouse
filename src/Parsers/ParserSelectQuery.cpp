#include <memory>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FIRST_AND_NEXT_TOGETHER;
    extern const int LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED;
    extern const int ROW_AND_ROWS_TOGETHER;
    extern const int SYNTAX_ERROR;
    extern const int TOP_AND_LIMIT_TOGETHER;
    extern const int WITH_TIES_WITHOUT_ORDER_BY;
    extern const int OFFSET_FETCH_WITHOUT_ORDER_BY;
}


bool ParserSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;

    ParserKeyword s_select(Keyword::SELECT);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_distinct(Keyword::DISTINCT);
    ParserKeyword s_distinct_on(Keyword::DISTINCT_ON);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_prewhere(Keyword::PREWHERE);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_group_by(Keyword::GROUP_BY);
    ParserKeyword s_with(Keyword::WITH);
    ParserKeyword s_recursive(Keyword::RECURSIVE);
    ParserKeyword s_totals(Keyword::TOTALS);
    ParserKeyword s_having(Keyword::HAVING);
    ParserKeyword s_window(Keyword::WINDOW);
    ParserKeyword s_qualify(Keyword::QUALIFY);
    ParserKeyword s_order_by(Keyword::ORDER_BY);
    ParserKeyword s_limit(Keyword::LIMIT);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_by(Keyword::BY);
    ParserKeyword s_rollup(Keyword::ROLLUP);
    ParserKeyword s_cube(Keyword::CUBE);
    ParserKeyword s_grouping_sets(Keyword::GROUPING_SETS);
    ParserKeyword s_top(Keyword::TOP);
    ParserKeyword s_with_ties(Keyword::WITH_TIES);
    ParserKeyword s_offset(Keyword::OFFSET);
    ParserKeyword s_fetch(Keyword::FETCH);
    ParserKeyword s_only(Keyword::ONLY);
    ParserKeyword s_row(Keyword::ROW);
    ParserKeyword s_rows(Keyword::ROWS);
    ParserKeyword s_first(Keyword::FIRST);
    ParserKeyword s_next(Keyword::NEXT);
    ParserKeyword s_interpolate(Keyword::INTERPOLATE);

    ParserNotEmptyExpressionList exp_list(false);
    ParserNotEmptyExpressionList exp_list_for_with_clause(false);
    ParserNotEmptyExpressionList exp_list_for_select_clause(/*allow_alias_without_as_keyword*/ true, /*allow_trailing_commas*/ true);
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserOrderByExpressionList order_list;
    ParserGroupingSetsExpressionList grouping_sets_list;
    ParserInterpolateExpressionList interpolate_list;

    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    ASTPtr with_expression_list;
    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr prewhere_expression;
    ASTPtr where_expression;
    ASTPtr group_expression_list;
    ASTPtr having_expression;
    ASTPtr window_list;
    ASTPtr qualify_expression;
    ASTPtr order_expression_list;
    ASTPtr interpolate_expression_list;
    ASTPtr limit_by_length;
    ASTPtr limit_by_offset;
    ASTPtr limit_by_expression_list;
    ASTPtr distinct_on_expression_list;
    ASTPtr limit_offset;
    ASTPtr limit_length;
    ASTPtr top_length;
    ASTPtr settings;

    /// WITH expr_list
    {
        if (s_with.ignore(pos, expected))
        {
            select_query->recursive_with = s_recursive.ignore(pos, expected);

            if (!ParserList(std::make_unique<ParserWithElement>(), std::make_unique<ParserToken>(TokenType::Comma))
                     .parse(pos, with_expression_list, expected))
                return false;
            if (with_expression_list->children.empty())
                return false;
        }
    }

    /// FROM database.table or FROM table or FROM (subquery) or FROM tableFunction(...)
    if (s_from.ignore(pos, expected))
    {
        if (!ParserTablesInSelectQuery(false).parse(pos, tables, expected))
            return false;
    }

    /// SELECT [ALL/DISTINCT [ON (expr_list)]] [TOP N [WITH TIES]] expr_list
    {
        bool has_all = false;
        if (!s_select.ignore(pos, expected))
        {
            /// This allows queries without SELECT, like `1 + 2`.
            if (!implicit_select || with_expression_list || tables)
                return false;
        }

        if (s_all.ignore(pos, expected))
            has_all = true;

        if (s_distinct_on.ignore(pos, expected))
        {
            if (open_bracket.ignore(pos, expected))
            {
                if (!exp_list.parse(pos, distinct_on_expression_list, expected))
                    return false;
                if (!close_bracket.ignore(pos, expected))
                    return false;
            }
            else
                return false;
        }
        else if (s_distinct.ignore(pos, expected))
        {
            select_query->distinct = true;
        }

        if (!has_all && s_all.ignore(pos, expected))
            has_all = true;

        if (has_all && (select_query->distinct || distinct_on_expression_list))
            return false;

        if (s_top.ignore(pos, expected))
        {
            ParserNumber num;

            if (open_bracket.ignore(pos, expected))
            {
                if (!num.parse(pos, top_length, expected))
                    return false;
                if (!close_bracket.ignore(pos, expected))
                    return false;
            }
            else
            {
                if (!num.parse(pos, top_length, expected))
                    return false;
            }

            if (s_with_ties.ignore(pos, expected))
                select_query->limit_with_ties = true;
        }

        if (!exp_list_for_select_clause.parse(pos, select_expression_list, expected))
            return false;
    }

    /// FROM database.table or FROM table or FROM (subquery) or FROM tableFunction(...)
    if (!tables && s_from.ignore(pos, expected))
    {
        if (!ParserTablesInSelectQuery().parse(pos, tables, expected))
            return false;
    }

    /// PREWHERE expr
    if (s_prewhere.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, prewhere_expression, expected))
            return false;
    }

    /// WHERE expr
    if (s_where.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, where_expression, expected))
            return false;
    }

    /// GROUP BY expr list
    if (s_group_by.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
            select_query->group_by_with_rollup = true;
        else if (s_cube.ignore(pos, expected))
            select_query->group_by_with_cube = true;
        else if (s_grouping_sets.ignore(pos, expected))
            select_query->group_by_with_grouping_sets = true;
        else if (s_all.ignore(pos, expected))
            select_query->group_by_all = true;

        if ((select_query->group_by_with_rollup || select_query->group_by_with_cube || select_query->group_by_with_grouping_sets) &&
            !open_bracket.ignore(pos, expected))
            return false;

        if (select_query->group_by_with_grouping_sets)
        {
            if (!grouping_sets_list.parse(pos, group_expression_list, expected))
                return false;
        }
        else if (!select_query->group_by_all)
        {
            if (!exp_list.parse(pos, group_expression_list, expected))
                return false;
        }


        if ((select_query->group_by_with_rollup || select_query->group_by_with_cube || select_query->group_by_with_grouping_sets) &&
            !close_bracket.ignore(pos, expected))
            return false;
    }

    /// WITH ROLLUP, CUBE, GROUPING SETS or TOTALS
    if (s_with.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
            select_query->group_by_with_rollup = true;
        else if (s_cube.ignore(pos, expected))
            select_query->group_by_with_cube = true;
        else if (s_totals.ignore(pos, expected))
            select_query->group_by_with_totals = true;
        else
            return false;
    }

    /// WITH TOTALS
    if (s_with.ignore(pos, expected))
    {
        if (select_query->group_by_with_totals || !s_totals.ignore(pos, expected))
            return false;

        select_query->group_by_with_totals = true;
    }

    /// HAVING expr
    if (s_having.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, having_expression, expected))
            return false;
    }

    /// WINDOW clause
    if (s_window.ignore(pos, expected))
    {
        ParserWindowList window_list_parser;
        if (!window_list_parser.parse(pos, window_list, expected))
        {
            return false;
        }
    }

    /// QUALIFY expr
    if (s_qualify.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, qualify_expression, expected))
            return false;
    }

    /// ORDER BY expr ASC|DESC COLLATE 'locale' list
    if (s_order_by.ignore(pos, expected))
    {
        if (!order_list.parse(pos, order_expression_list, expected))
            return false;

        /// if any WITH FILL parse possible INTERPOLATE list
        if (std::any_of(order_expression_list->children.begin(), order_expression_list->children.end(),
                [](auto & child) { return child->template as<ASTOrderByElement>()->with_fill; }))
        {
            if (s_interpolate.ignore(pos, expected))
            {
                if (open_bracket.ignore(pos, expected))
                {
                    if (!interpolate_list.parse(pos, interpolate_expression_list, expected))
                        return false;
                    if (!close_bracket.ignore(pos, expected))
                        return false;
                } else
                    interpolate_expression_list = std::make_shared<ASTExpressionList>();
            }
        }
        else if (order_expression_list->children.size() == 1)
        {
            /// ORDER BY ALL
            auto * identifier = order_expression_list->children[0]->as<ASTOrderByElement>()->children[0]->as<ASTIdentifier>();
            if (identifier != nullptr && Poco::toUpper(identifier->name()) == "ALL")
                select_query->order_by_all = true;
        }
    }

    /// This is needed for TOP expression, because it can also use WITH TIES.
    bool limit_with_ties_occured = false;

    bool has_offset_clause = false;
    bool offset_clause_has_sql_standard_row_or_rows = false; /// OFFSET offset_row_count {ROW | ROWS}

    /// LIMIT length | LIMIT offset, length | LIMIT count BY expr-list | LIMIT offset, length BY expr-list
    if (s_limit.ignore(pos, expected))
    {
        ParserToken s_comma(TokenType::Comma);

        if (!exp_elem.parse(pos, limit_length, expected))
            return false;

        if (s_comma.ignore(pos, expected))
        {
            limit_offset = limit_length;
            if (!exp_elem.parse(pos, limit_length, expected))
                return false;

            if (s_with_ties.ignore(pos, expected))
            {
                limit_with_ties_occured = true;
                select_query->limit_with_ties = true;
            }
        }
        else if (s_offset.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, limit_offset, expected))
                return false;

            has_offset_clause = true;
        }
        else if (s_with_ties.ignore(pos, expected))
        {
            limit_with_ties_occured = true;
            select_query->limit_with_ties = true;
        }

        if (limit_with_ties_occured && distinct_on_expression_list)
            throw Exception(ErrorCodes::LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED, "Can not use WITH TIES alongside LIMIT BY/DISTINCT ON");

        if (s_by.ignore(pos, expected))
        {
            /// WITH TIES was used alongside LIMIT BY
            /// But there are other kind of queries like LIMIT n BY smth LIMIT m WITH TIES which are allowed.
            /// So we have to ignore WITH TIES exactly in LIMIT BY state.
            if (limit_with_ties_occured)
                throw Exception(ErrorCodes::LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED, "Can not use WITH TIES alongside LIMIT BY/DISTINCT ON");

            if (distinct_on_expression_list)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use DISTINCT ON alongside LIMIT BY");

            limit_by_length = limit_length;
            limit_by_offset = limit_offset;
            limit_length = nullptr;
            limit_offset = nullptr;

            if (!exp_list.parse(pos, limit_by_expression_list, expected))
                return false;
        }

        if (top_length && limit_length)
            throw Exception(ErrorCodes::TOP_AND_LIMIT_TOGETHER, "Can not use TOP and LIMIT together");
    }
    else if (s_offset.ignore(pos, expected))
    {
        /// OFFSET without LIMIT

        has_offset_clause = true;

        if (!exp_elem.parse(pos, limit_offset, expected))
            return false;

        /// SQL standard OFFSET N ROW[S] ...

        if (s_row.ignore(pos, expected))
            offset_clause_has_sql_standard_row_or_rows = true;

        if (s_rows.ignore(pos, expected))
        {
            if (offset_clause_has_sql_standard_row_or_rows)
                throw Exception(ErrorCodes::ROW_AND_ROWS_TOGETHER, "Can not use ROW and ROWS together");

            offset_clause_has_sql_standard_row_or_rows = true;
        }
    }

    /// SQL standard FETCH (either following SQL standard OFFSET or following ORDER BY)
    if ((!has_offset_clause || offset_clause_has_sql_standard_row_or_rows)
        && s_fetch.ignore(pos, expected))
    {
        /// FETCH clause must exist with "ORDER BY"
        if (!order_expression_list)
            throw Exception(ErrorCodes::OFFSET_FETCH_WITHOUT_ORDER_BY, "Can not use OFFSET FETCH clause without ORDER BY");

        if (s_first.ignore(pos, expected))
        {
            if (s_next.ignore(pos, expected))
                throw Exception(ErrorCodes::FIRST_AND_NEXT_TOGETHER, "Can not use FIRST and NEXT together");
        }
        else if (!s_next.ignore(pos, expected))
            return false;

        if (!exp_elem.parse(pos, limit_length, expected))
            return false;

        if (s_row.ignore(pos, expected))
        {
            if (s_rows.ignore(pos, expected))
                throw Exception(ErrorCodes::ROW_AND_ROWS_TOGETHER, "Can not use ROW and ROWS together");
        }
        else if (!s_rows.ignore(pos, expected))
            return false;

        if (s_with_ties.ignore(pos, expected))
        {
            select_query->limit_with_ties = true;
        }
        else if (s_only.ignore(pos, expected))
        {
            select_query->limit_with_ties = false;
        }
        else
        {
            return false;
        }
    }

    if (distinct_on_expression_list)
    {
        /// DISTINCT ON and LIMIT BY are mutually exclusive, checked before
        assert (limit_by_expression_list == nullptr);

        /// Transform `DISTINCT ON expr` to `LIMIT 1 BY expr`
        limit_by_expression_list = distinct_on_expression_list;
        limit_by_length = std::make_shared<ASTLiteral>(Field{static_cast<UInt8>(1)});
        distinct_on_expression_list = nullptr;
    }

    /// Because TOP n in totally equals LIMIT n
    if (top_length)
        limit_length = top_length;

    /// LIMIT length [WITH TIES] | LIMIT offset, length [WITH TIES]
    if (s_limit.ignore(pos, expected))
    {
        if (!limit_by_length || limit_length)
            return false;

        ParserToken s_comma(TokenType::Comma);

        if (!exp_elem.parse(pos, limit_length, expected))
            return false;

        if (s_comma.ignore(pos, expected))
        {
            limit_offset = limit_length;
            if (!exp_elem.parse(pos, limit_length, expected))
                return false;
        }
        else if (s_offset.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, limit_offset, expected))
                return false;
        }

        if (s_with_ties.ignore(pos, expected))
            select_query->limit_with_ties = true;
    }

    /// WITH TIES was used without ORDER BY
    if (!order_expression_list && select_query->limit_with_ties)
        throw Exception(ErrorCodes::WITH_TIES_WITHOUT_ORDER_BY, "Can not use WITH TIES without ORDER BY");

    /// SETTINGS key1 = value1, key2 = value2, ...
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, settings, expected))
            return false;
    }

    select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, std::move(prewhere_expression));
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::HAVING, std::move(having_expression));
    select_query->setExpression(ASTSelectQuery::Expression::WINDOW, std::move(window_list));
    select_query->setExpression(ASTSelectQuery::Expression::QUALIFY, std::move(qualify_expression));
    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, std::move(limit_by_offset));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, std::move(limit_by_length));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY, std::move(limit_by_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(limit_offset));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));
    select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));
    select_query->setExpression(ASTSelectQuery::Expression::INTERPOLATE, std::move(interpolate_expression_list));
    return true;
}

}
