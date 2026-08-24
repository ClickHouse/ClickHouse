#include <algorithm>
#include <memory>
#include <string_view>
#include <base/defines.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


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


bool parseOrderByClauseBody(
    IParser::Pos & pos,
    Expected & expected,
    ASTPtr & order_expression_list,
    ASTPtr & interpolate_expression_list,
    bool & order_by_all)
{
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_first(Keyword::FIRST);
    ParserKeyword s_interpolate(Keyword::INTERPOLATE);
    ParserOrderByExpressionList order_list;
    ParserInterpolateExpressionList interpolate_list;
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    order_by_all = false;

    /// ParserKeyword only matches BareWord tokens, so quoted identifiers like `all` won't match.
    /// This allows ORDER BY `all` to refer to a column named "all" rather than ORDER BY ALL.
    ///
    /// After matching the ALL keyword with optional ASC/DESC/NULLS modifiers,
    /// we check if a comma follows. If so, this is a multi-column ORDER BY (e.g. ORDER BY all, a)
    /// and `all` should be treated as a regular column reference, not the ALL keyword.
    auto saved_pos = pos;

    if (s_all.ignore(pos, expected))
    {
        order_by_all = true;

        /// Parse the optional ASC/DESC and NULLS direction after ORDER BY ALL.
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
            if (s_first.ignore(pos, expected))
                nulls_direction = -direction;
            else if (s_last.ignore(pos, expected))
                ;
            else
                return false;
        }

        /// If a comma follows, this is a multi-column ORDER BY (e.g., ORDER BY all, a).
        /// In this case, `all` should be treated as a regular column, not the ALL keyword.
        if (pos->type == TokenType::Comma)
        {
            /// Backtrack to before we consumed `ALL`.
            pos = saved_pos;
            order_by_all = false;
        }
        else
        {
            auto elem = make_intrusive<ASTOrderByElement>();
            elem->direction = direction;
            elem->nulls_direction = nulls_direction;
            elem->nulls_direction_was_explicitly_specified = nulls_direction_was_explicitly_specified;
            elem->children.push_back(make_intrusive<ASTIdentifier>("all"));

            order_expression_list = make_intrusive<ASTExpressionList>();
            order_expression_list->children.push_back(std::move(elem));
        }
    }

    if (!order_by_all)
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
                }
                else
                    interpolate_expression_list = make_intrusive<ASTExpressionList>();
            }
        }
    }

    return true;
}


namespace
{

bool isSelectKeyword(std::string_view str)
{
    return equalsCaseInsensitive(str, "select");
}

/// Whether one of the top-level table expressions has the alias `select` (in any letter case).
/// In the FROM-first form of a query, the tables are first parsed allowing aliases without the AS
/// keyword; the only ambiguity is the SELECT keyword that starts the explicit SELECT clause being
/// consumed as such an alias (SELECT is the only clause keyword that is not in
/// ParserAlias::restricted_keywords). Only the aliases of the table expressions themselves can be
/// that SELECT keyword, so an alias inside a subquery or a table function
/// (FROM (SELECT 1 AS select) s) must not trigger the reparse, and neither must a relation named
/// `select` (FROM select s), whose own alias is something else.
bool hasTopLevelSelectAlias(const ASTPtr & tables)
{
    if (!tables)
        return false;

    auto is_aliased_as_select = [](const ASTPtr & ast) { return ast && isSelectKeyword(ast->tryGetAlias()); };

    for (const auto & child : tables->children)
    {
        const auto * tables_element = child->as<ASTTablesInSelectQueryElement>();
        if (!tables_element || !tables_element->table_expression)
            continue;

        const auto & table_expression = tables_element->table_expression->as<ASTTableExpression &>();
        if (is_aliased_as_select(table_expression.subquery) || is_aliased_as_select(table_expression.table_function)
            || is_aliased_as_select(table_expression.database_and_table_name))
            return true;
    }

    return false;
}

/// Whether the last token of the range (the last token consumed by the tables parse) is a bareword
/// `select` (in any letter case). Only the alias that ends the tables parse can be the swallowed
/// clause-start SELECT: everything after it would be the SELECT clause, which the tables parser
/// cannot consume. An alias `select` elsewhere in the FROM clause (e.g. a joined-table alias
/// followed by its ON section: FROM a JOIN b select ON a.x = select.x) is unambiguous and must not
/// trigger the reparse, and neither must a quoted alias (FROM orders `select`), which is not
/// a BareWord token.
bool endsWithBarewordSelect(TokenIterator begin, TokenIterator end)
{
    if (!(begin < end))
        return false;

    --end;
    return end->type == TokenType::BareWord && isSelectKeyword({end->begin, end->size()});
}

/// Whether the last top-level table expression is sampled with an offset, so that the OFFSET is the
/// last thing consumed by the tables parse: FROM t SAMPLE 1/10 OFFSET 5. Only in this position the
/// consumed OFFSET could have been meant as a query-level OFFSET instead. A sample offset that is
/// followed by more of the FROM clause (FROM t SAMPLE 1/10 OFFSET 5 JOIN dim USING (id)) is
/// unambiguous - a query-level OFFSET cannot appear there.
bool lastTableExpressionHasSampleOffset(const ASTPtr & tables)
{
    if (!tables || tables->children.empty())
        return false;

    const auto * tables_element = tables->children.back()->as<ASTTablesInSelectQueryElement>();
    if (!tables_element || !tables_element->table_expression)
        return false;

    return tables_element->table_expression->as<ASTTableExpression &>().sample_offset != nullptr;
}

/// Whether the query continues with a clause that can only precede a query-level OFFSET. If it does,
/// a preceding OFFSET must have been the sample offset, so the omitted-SELECT form is unambiguous.
bool nextClauseCannotFollowQueryLevelOffset(IParser::Pos & pos, Expected & expected)
{
    static const Keyword clause_keywords[] = {
        Keyword::PREWHERE,
        Keyword::WHERE,
        Keyword::GROUP_BY,
        Keyword::HAVING,
        Keyword::WINDOW,
        Keyword::QUALIFY,
        Keyword::ORDER_BY,
        Keyword::LIMIT,
    };

    for (auto keyword : clause_keywords)
    {
        if (ParserKeyword(keyword).checkWithoutMoving(pos, expected))
            return true;
    }

    return false;
}

}


bool ParserSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = make_intrusive<ASTSelectQuery>();
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

    ParserNotEmptyExpressionList exp_list(false);
    ParserNotEmptyExpressionList exp_list_for_with_clause(false);
    ParserNotEmptyExpressionList exp_list_for_select_clause(/*allow_alias_without_as_keyword*/ true, /*allow_trailing_commas*/ true);
    ParserAliasesExpressionList exp_list_for_aliases;
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserGroupingSetsExpressionList grouping_sets_list;

    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    ASTPtr with_expression_list;
    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr expression_list_for_aliases;
    ASTPtr expression_list_for_cte_aliases;
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

            if (!ParserList(std::make_unique<ParserWithElement>(), std::make_unique<ParserToken>(TokenType::Comma), true, ',', true)
                     .parse(pos, with_expression_list, expected))
                return false;
            if (with_expression_list->children.empty())
                return false;

            for (const auto & child : with_expression_list->children) /// For cases: WITH _ (a, b) AS ...      <- (a, b) are aliases
            {
                if (auto * with_element = child->as<ASTWithElement>())
                    if (with_element->aliases)
                        expression_list_for_cte_aliases = with_element->aliases;
            }
        }
    }

    /// FROM database.table or FROM table or FROM (subquery) or FROM tableFunction(...)
    if (s_from.ignore(pos, expected))
    {
        Pos tables_end = pos;
        auto parse_tables_and_alias_list = [&](bool allow_alias_without_as_keyword)
        {
            if (!ParserTablesInSelectQuery(allow_alias_without_as_keyword).parse(pos, tables, expected))
                return false;

            tables_end = pos;

            /// In the FROM-first form, an optional column alias list can follow the tables, before the
            /// (possibly omitted) SELECT clause, e.g. FROM t (a, b) SELECT a. It must be parsed here,
            /// before deciding whether the SELECT clause was omitted, otherwise the alias list would be
            /// mistaken for the start of an implicit-SELECT query and the following SELECT would fail.
            if (open_bracket.ignore(pos, expected))
            {
                if (!exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected))
                    return false;

                if (!close_bracket.ignore(pos, expected))
                    return false;
            }

            return true;
        };

        /// Table aliases without the AS keyword are allowed, same as in the FROM clause of an
        /// ordinary SELECT query: FROM orders o SELECT o.customer, FROM orders o WHERE o.amount > 0.
        /// All clause keywords except SELECT itself are restricted aliases, so the only ambiguity is
        /// the SELECT keyword consumed as an alias: in FROM t SELECT expr the permissive parse makes
        /// "SELECT" the alias of t. When that could have happened and no explicit SELECT clause
        /// follows, roll back and reparse with aliases requiring the AS keyword, so that SELECT
        /// starts the clause. The reparse is triggered only when a top-level table expression got the
        /// alias `select` and the tables parse ended right after a bareword `select` - the only
        /// position where the clause-start SELECT can be swallowed as an alias. A relation named
        /// `select` (FROM select s), a quoted alias (FROM orders `select`) and an alias `select`
        /// followed by more of the FROM clause (FROM a JOIN b select ON a.x = select.x) keep their
        /// permissive parse. When the bareword `select` turns out to be an explicit AS alias, the
        /// strict reparse yields the same result anyway.
        auto begin = pos;
        if (!parse_tables_and_alias_list(/*allow_alias_without_as_keyword*/ true)
            || (!s_select.checkWithoutMoving(pos, expected) && hasTopLevelSelectAlias(tables)
                && endsWithBarewordSelect(begin, tables_end)))
        {
            pos = begin;
            tables = nullptr;
            expression_list_for_aliases = nullptr;

            if (!parse_tables_and_alias_list(/*allow_alias_without_as_keyword*/ false))
                return false;
        }
    }

    /// SELECT [ALL/DISTINCT [ON (expr_list)]] [TOP N [WITH TIES]] expr_list
    bool has_select_keyword = s_select.ignore(pos, expected);
    if (!has_select_keyword && tables)
    {
        /// FROM t SAMPLE 1/10 OFFSET 5 is ambiguous when SELECT is omitted: the OFFSET has already been
        /// consumed as the SAMPLE offset, while in the explicit form FROM t SAMPLE 1/10 SELECT * OFFSET 5
        /// it is a query-level OFFSET (the formatter of ASTSelectQuery relies on the explicit form to
        /// disambiguate). Reject the omitted-SELECT form for this shape and require an explicit SELECT.
        /// The ambiguity exists only for the sample offset that ends the tables parse and only when the
        /// query does not continue with a clause that a query-level OFFSET could not precede.
        if (lastTableExpressionHasSampleOffset(tables) && !nextClauseCannotFollowQueryLevelOffset(pos, expected))
        {
            expected.add(pos, "SELECT (it cannot be omitted when the last table has SAMPLE with OFFSET)");
            return false;
        }

        /// A query that starts with the FROM clause can omit SELECT - then it is equivalent to SELECT *.
        /// This form is mostly used in queries with pipe operators: FROM t |> WHERE x |> LIMIT 1.
        auto asterisk_list = make_intrusive<ASTExpressionList>();
        asterisk_list->children.push_back(make_intrusive<ASTAsterisk>());
        select_expression_list = std::move(asterisk_list);
    }
    else
    {
        bool has_all = false;

        /// This allows queries without SELECT, like `1 + 2`.
        if (!has_select_keyword && (!implicit_select || with_expression_list))
            return false;

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

    if (tables && open_bracket.ignore(pos, expected))
    {
        if (!exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected))
            return false;

        if (!close_bracket.ignore(pos, expected))
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

    /// WITH ROLLUP, CUBE, or TOTALS (multiple modifiers allowed, e.g. WITH ROLLUP WITH CUBE WITH TOTALS)
    while (s_with.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
        {
            if (select_query->group_by_with_rollup)
                return false;
            select_query->group_by_with_rollup = true;
        }
        else if (s_cube.ignore(pos, expected))
        {
            if (select_query->group_by_with_cube)
                return false;
            select_query->group_by_with_cube = true;
        }
        else if (s_totals.ignore(pos, expected))
        {
            if (select_query->group_by_with_totals)
                return false;
            select_query->group_by_with_totals = true;
        }
        else
            return false;
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
        bool order_by_all = false;
        if (!parseOrderByClauseBody(pos, expected, order_expression_list, interpolate_expression_list, order_by_all))
            return false;
        select_query->order_by_all = order_by_all;
    }

    /// This is needed for TOP expression, because it can also use WITH TIES.
    bool limit_with_ties_occurred = false;

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
                limit_with_ties_occurred = true;
                select_query->limit_with_ties = true;
            }
        }
        else if (s_offset.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, limit_offset, expected))
                return false;

            has_offset_clause = true;

            if (s_with_ties.ignore(pos, expected))
            {
                limit_with_ties_occurred = true;
                select_query->limit_with_ties = true;
            }
        }
        else if (s_with_ties.ignore(pos, expected))
        {
            limit_with_ties_occurred = true;
            select_query->limit_with_ties = true;
        }

        if (limit_with_ties_occurred && distinct_on_expression_list)
            throw Exception(ErrorCodes::LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED, "Can not use WITH TIES alongside LIMIT BY/DISTINCT ON");

        if (s_by.ignore(pos, expected))
        {
            /// WITH TIES was used alongside LIMIT BY
            /// But there are other kind of queries like LIMIT n BY smth LIMIT m WITH TIES which are allowed.
            /// So we have to ignore WITH TIES exactly in LIMIT BY state.
            if (limit_with_ties_occurred)
                throw Exception(ErrorCodes::LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED, "Can not use WITH TIES alongside LIMIT BY/DISTINCT ON");

            if (distinct_on_expression_list)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use DISTINCT ON alongside LIMIT BY");

            limit_by_length = limit_length;
            limit_by_offset = limit_offset;
            limit_length = nullptr;
            limit_offset = nullptr;

            if (s_all.ignore(pos, expected))
            {
                select_query->limit_by_all = true;
                limit_by_expression_list = make_intrusive<ASTExpressionList>();
            }
            else
            {
                if (!exp_list.parse(pos, limit_by_expression_list, expected))
                    return false;
            }
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
        chassert(limit_by_expression_list == nullptr);

        /// Transform `DISTINCT ON expr` to `LIMIT 1 BY expr`
        limit_by_expression_list = distinct_on_expression_list;
        limit_by_length = make_intrusive<ASTLiteral>(Field{static_cast<UInt8>(1)});
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
    select_query->setExpression(ASTSelectQuery::Expression::ALIASES, std::move(expression_list_for_aliases));
    select_query->setExpression(ASTSelectQuery::Expression::CTE_ALIASES, std::move(expression_list_for_cte_aliases));
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

namespace DB
{

void registerStatementSelect(StatementFactory & factory)
{
    factory.registerStatement("SELECT",
    {
        .description = R"(
`SELECT` queries perform data retrieval. By default, the requested data is returned to the client, while in
conjunction with `INSERT INTO` it can be forwarded to a different table.

**Examples**

**Select rows of a table**

```sql title="Query"
SELECT * FROM numbers(3);
```

```response title="Response"
0
1
2
```
)",
        .syntax = R"(
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION ALL|DISTINCT ...]
[INTO OUTFILE filename [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
)",
        .related = {"FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "JOIN", "UNION", "INSERT INTO", "FORMAT"},
    });

    factory.registerStatement("DISTINCT",
    {
        .description = R"(
Removes duplicates from the result: only a single row remains out of all the sets of fully matching rows. The list of
columns which must have unique values can be specified with `SELECT DISTINCT ON (column1, column2, ...)`; if the
columns are not specified, all of them are taken into account.

**Examples**

**Remove duplicate rows**

```sql title="Query"
SELECT DISTINCT * FROM t1;
```
)",
        .syntax = R"(
SELECT DISTINCT [ON (column1, column2, ...)] expr_list ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "ALL", "GROUP BY", "LIMIT BY"},
    });

    factory.registerStatement("ALL",
    {
        .description = R"(
If there are multiple matching rows in a table, `ALL` returns all of them. `SELECT ALL` is identical to `SELECT`
without `DISTINCT`; specifying both `ALL` and `DISTINCT` raises an exception. `ALL` can also be specified inside an
aggregate function, where it has no effect on the result.

**Examples**

**Use ALL inside an aggregate function**

```sql title="Query"
SELECT sum(ALL number) FROM numbers(10);
```

```response title="Response"
45
```
)",
        .syntax = R"(
SELECT ALL expr_list ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "DISTINCT"},
    });

    factory.registerStatement("PREWHERE",
    {
        .description = R"(
Filters the data before reading all the columns of the query: ClickHouse first reads only the columns needed to
evaluate the `PREWHERE` expression, and then reads the other columns only for the rows which satisfy it. This makes
filtering more efficient by reducing the amount of data read.

By default, ClickHouse applies this optimization automatically by moving eligible conditions from `WHERE` to
`PREWHERE`; specify `PREWHERE` explicitly to control which conditions are applied at this stage.

**Examples**

**Filter before reading the other columns**

```sql title="Query"
SELECT id, value FROM table_1 PREWHERE id >= 2 ORDER BY id;
```
)",
        .syntax = R"(
SELECT ... PREWHERE expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "WHERE", "EXPLAIN"},
    });

    factory.registerStatement("WHERE",
    {
        .description = R"(
Filters the data which comes from the `FROM` clause. The expression must have type `UInt8`; the rows for which it
evaluates to `0` are excluded from further transformations and from the result.

If the table has an index, and the condition is compatible with it, then only the parts of the data which can satisfy
the condition are read.

**Examples**

**Filter rows by a condition**

```sql title="Query"
SELECT * FROM t_null WHERE y IS NULL;
```
)",
        .syntax = R"(
SELECT ... WHERE expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "PREWHERE", "HAVING", "QUALIFY"},
    });

    factory.registerStatement("GROUP BY",
    {
        .description = R"(
Switches the query into aggregation mode: the rows are grouped by the values of the grouping key, and the aggregate
functions of the query are calculated over each group. The `WITH ROLLUP`, `WITH CUBE` and `WITH TOTALS` modifiers
additionally produce subtotals.

**Examples**

**Calculate subtotals**

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```
)",
        .syntax = R"(
SELECT ... GROUP BY expr_list [WITH ROLLUP | WITH CUBE] [WITH TOTALS] ...
SELECT ... GROUP BY ROLLUP(expr_list) | CUBE(expr_list) | GROUPING SETS (...) ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "HAVING", "DISTINCT", "ORDER BY"},
    });

    factory.registerStatement("HAVING",
    {
        .description = R"(
Filters the aggregation results produced by `GROUP BY`. It is similar to the `WHERE` clause, but `WHERE` is performed
before the aggregation, whereas `HAVING` is performed after it. The aggregation results can be referenced by their
alias from the `SELECT` clause.

**Examples**

**Filter the aggregated rows**

```sql title="Query"
SELECT region, sum(amount) AS total_sales
FROM sales
GROUP BY region
HAVING total_sales > 10000;
```
)",
        .syntax = R"(
SELECT ... GROUP BY ... HAVING expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "GROUP BY", "WHERE", "QUALIFY"},
    });

    factory.registerStatement("QUALIFY",
    {
        .description = R"(
Filters the results of window functions. It is similar to the `WHERE` clause, but `WHERE` is performed before the
window functions are evaluated, whereas `QUALIFY` is performed after it. The results of the window functions can be
referenced by their alias from the `SELECT` clause.

**Examples**

**Filter by the result of a window function**

```sql title="Query"
SELECT number, count() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```
)",
        .syntax = R"(
SELECT ... QUALIFY expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "WHERE", "HAVING"},
    });

    factory.registerStatement("ORDER BY",
    {
        .description = R"(
Sorts the result. The sorting key can be a list of expressions, a list of numbers referring to the columns of the
`SELECT` clause, or `ALL`, which means all columns of the `SELECT` clause. `NULL` values are ordered with
`NULLS FIRST` or `NULLS LAST`, strings can be compared according to a collation, and gaps in the sorted sequence can
be filled with `WITH FILL`.

**Examples**

**Sort with a collation**

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```
)",
        .syntax = R"(
SELECT ... ORDER BY expr [ASC | DESC] [NULLS FIRST | NULLS LAST] [COLLATE 'locale'] [, ...]
    [WITH FILL [FROM expr] [TO expr] [STEP expr] [STALENESS expr]] [INTERPOLATE [(expr_list)]] ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "LIMIT", "GROUP BY", "ALTER TABLE ... MODIFY ORDER BY"},
    });

    factory.registerStatement("LIMIT",
    {
        .description = R"(
Controls how many rows are returned. `LIMIT m` returns the first `m` rows, `LIMIT n, m` and `LIMIT m OFFSET n` skip
the first `n` rows and return the next `m` rows. `WITH TIES` additionally returns the rows which have the same sorting
key as the last returned row.

Without `ORDER BY`, the result is non-deterministic.

**Examples**

**Return the first 10 rows**

```sql title="Query"
SELECT * FROM numbers(100) LIMIT 10;
```
)",
        .syntax = R"(
SELECT ... LIMIT m [WITH TIES]
SELECT ... LIMIT n, m [WITH TIES]
SELECT ... LIMIT m OFFSET n [WITH TIES]
SELECT TOP m ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "OFFSET FETCH", "LIMIT BY", "ORDER BY"},
    });

    factory.registerStatement("LIMIT BY",
    {
        .description = R"(
Selects the first `n` rows for each distinct value of the expressions of the clause. `LIMIT BY` is applied before
`LIMIT`, and can be combined with an offset.

**Examples**

**Take two rows per group**

```sql title="Query"
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```
)",
        .syntax = R"(
SELECT ... LIMIT [offset_value, ]n BY expressions ...
SELECT ... LIMIT n OFFSET offset_value BY expressions ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "LIMIT", "DISTINCT", "ORDER BY"},
    });

    factory.registerStatement("OFFSET FETCH",
    {
        .description = R"(
Retrieves the result by portions: skips `offset_row_count` rows and returns the next `fetch_row_count` rows. This is
the SQL standard spelling of `LIMIT` with an offset. `WITH TIES` additionally returns the rows which have the same
sorting key as the last returned row.

**Examples**

**Skip one row and fetch three rows**

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```
)",
        .syntax = R"(
SELECT ... [OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]
SELECT ... [LIMIT [n, ]m] [OFFSET offset_row_count]
)",
        .parent = "SELECT",
        .related = {"SELECT", "LIMIT", "ORDER BY"},
    });
}

}
