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
        .description = R"DOCS_MD(
`SELECT` queries perform data retrieval. By default, the requested data is returned to the client, while in conjunction with [INSERT INTO](/reference/statements/insert-into) it can be forwarded to a different table.

## Syntax {#syntax}

```sql
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
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

The expression list immediately after `SELECT` is required; all other clauses are optional.

`SELECT` and its optional clauses are covered in separate sections:

- [WITH clause](/reference/statements/select/with)
- [SELECT clause](#select-clause)
- [ALL clause](/reference/statements/select/all)
- [DISTINCT clause](/reference/statements/select/distinct)
- [FROM clause](/reference/statements/select/from)
- [SAMPLE clause](/reference/statements/select/sample)
- [ARRAY JOIN clause](/reference/statements/select/array-join)
- [JOIN clause](/reference/statements/select/join)
- [PREWHERE clause](/reference/statements/select/prewhere)
- [WHERE clause](/reference/statements/select/where)
- [GROUP BY clause](/reference/statements/select/group-by)
- [HAVING clause](/reference/statements/select/having)
- [WINDOW clause](/reference/functions/window-functions/index)
- [QUALIFY clause](/reference/statements/select/qualify)
- [ORDER BY clause](/reference/statements/select/order-by)
- [LIMIT BY clause](/reference/statements/select/limit-by)
- [LIMIT clause](/reference/statements/select/limit)
- [OFFSET clause](/reference/statements/select/offset)
- [UNION clause](/reference/statements/select/union)
- [INTERSECT clause](/reference/statements/select/intersect)
- [EXCEPT clause](/reference/statements/select/except)
- [INTO OUTFILE clause](/reference/statements/select/into-outfile)
- [FORMAT clause](/reference/statements/select/format)

A query can also be written as a linear chain of transformations with [pipe operators](/reference/statements/select/pipe-operators):

```sql
FROM table
|> WHERE x > 1
|> AGGREGATE count() AS c GROUP BY y
|> ORDER BY c DESC
```

## SELECT Clause {#select-clause}

[Expressions](/reference/syntax#expressions) specified in the `SELECT` clause are calculated after all the operations in the clauses described above are finished. These expressions work as if they apply to separate rows in the result. If expressions in the `SELECT` clause contain aggregate functions, then ClickHouse processes aggregate functions and expressions used as their arguments during the [GROUP BY](/reference/statements/select/group-by) aggregation.

If you want to include all columns in the result, use the asterisk (`*`) symbol. For example, `SELECT * FROM ...`.

### Dynamic column selection {#dynamic-column-selection}

Dynamic column selection (also known as a COLUMNS expression) allows you to match some columns in a result with a [re2](https://en.wikipedia.org/wiki/RE2_(software)) regular expression.

```sql
COLUMNS('regexp')
```

For example, consider the table:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

The following query selects data from all the columns containing the `a` symbol in their name.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

The selected columns are returned not in the alphabetical order.

You can use multiple `COLUMNS` expressions in a query and apply functions to them.

For example:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Each column returned by the `COLUMNS` expression is passed to the function as a separate argument. Also you can pass other arguments to the function if it supports them. Be careful when using functions. If a function does not support the number of arguments you have passed to it, ClickHouse throws an exception.

For example:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

In this example, `COLUMNS('a')` returns two columns: `aa` and `ab`. `COLUMNS('c')` returns the `bc` column. The `+` operator can't apply to 3 arguments, so ClickHouse throws an exception with the relevant message.

Columns that matched the `COLUMNS` expression can have different data types. If `COLUMNS` does not match any columns and is the only expression in `SELECT`, ClickHouse throws an exception.

#### Select columns with `LIKE` or `ILIKE` {#select-columns-with-like-or-ilike}

You can also select columns by matching their names against a pattern after `*`, using a case-sensitive `LIKE` or a case-insensitive `ILIKE`:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

The `LIKE` and `ILIKE` patterns follow `LIKE` semantics, not regular expression semantics. The `%` character matches any sequence of characters, the `_` character matches any single character, and `\` escapes `%`, `_`, and `\`. The only difference between the two is that `LIKE` matches column names case-sensitively, while `ILIKE` is case-insensitive. For example:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

The query selects columns with two-character names that start with `a`, such as `aa` and `ab`.

`* LIKE` and `* ILIKE` also support qualified asterisks and column transformers:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

### Asterisk {#asterisk}

You can put an asterisk in any part of a query instead of an expression. When the query is analyzed, the asterisk is expanded to a list of all table columns (excluding the `MATERIALIZED` and `ALIAS` columns). There are only a few cases when using an asterisk is justified:

- When creating a table dump.
- For tables containing just a few columns, such as system tables.
- For getting information about what columns are in a table. In this case, set `LIMIT 1`. But it is better to use the `DESC TABLE` query.
- When there is strong filtration on a small number of columns using `PREWHERE`.
- In subqueries (since columns that aren't needed for the external query are excluded from subqueries).

In all other cases, we do not recommend using the asterisk, since it only gives you the drawbacks of a columnar DBMS instead of the advantages. In other words using the asterisk is not recommended.

### Extreme Values {#extreme-values}

In addition to results, you can also get minimum and maximum values for the results columns. To do this, set the **extremes** setting to 1. Minimums and maximums are calculated for numeric types, dates, and dates with times. For other columns, the default values are output.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template` and `Pretty*` [formats](/reference/formats/index), separate from the other rows. They are not output for other formats.

In `JSON*` and `XML` formats, the extreme values are output in a separate 'extremes' field. In `TabSeparated*`, `CSV*` and `Vertical` formats, the row comes after the main result, and after 'totals' if present. It is preceded by an empty row (after the other data). In `Pretty*` formats, the row is output as a separate table after the main result, and after `totals` if present. In `Template` format the extreme values are output according to specified template.

Extreme values are calculated for rows before `LIMIT`, but after `LIMIT BY`. However, when using `LIMIT offset, size`, the rows before `offset` are included in `extremes`. In stream requests, the result may also include a small number of rows that passed through `LIMIT`.

### Notes {#notes}

You can use synonyms (`AS` aliases) in any part of a query.

The `GROUP BY`, `ORDER BY`, and `LIMIT BY` clauses can support positional arguments. To enable this, switch on the [enable_positional_arguments](/reference/settings/session-settings/enable-positional-arguments#enable_positional_arguments) setting. Then, for example, `ORDER BY 1,2` will be sorting rows in the table on the first and then the second column.

## Implementation Details {#implementation-details}

If the query omits the `DISTINCT`, `GROUP BY` and `ORDER BY` clauses and the `IN` and `JOIN` subqueries, the query will be completely stream processed, using O(1) amount of RAM. Otherwise, the query might consume a lot of RAM if the appropriate restrictions are not specified:

- `max_memory_usage`
- `max_rows_to_group_by`
- `max_rows_to_sort`
- `max_rows_in_distinct`
- `max_bytes_in_distinct`
- `max_rows_in_set`
- `max_bytes_in_set`
- `max_rows_in_join`
- `max_bytes_in_join`
- `max_bytes_before_external_sort`
- `max_bytes_ratio_before_external_sort`
- `max_bytes_before_external_group_by`
- `max_bytes_ratio_before_external_group_by`

For more information, see the section "Settings". It is possible to use external sorting (saving temporary tables to a disk) and external aggregation.

## SELECT modifiers {#select-modifiers}

You can use the following modifiers in `SELECT` queries.

| Modifier                            | Description                                                                                                                                                                                                                                                                                                                                                                              |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`APPLY`](/reference/statements/select/apply_modifier)     | Allows you to invoke some function for each row returned by an outer table expression of a query.                                                                                                                                                                                                                                                                                        |
| [`EXCEPT`](/reference/statements/select/except_modifier)   | Specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.                                                                                                                                                                                                                                                            |
| [`REPLACE`](/reference/statements/select/replace_modifier) | Specifies one or more [expression aliases](/reference/syntax#expression-aliases). Each alias must match a column name from the `SELECT *` statement. In the output column list, the column that matches the alias is replaced by the expression in that `REPLACE`. This modifier does not change the names or order of columns. However, it can change the value and the value type. |

### Modifier Combinations {#modifier-combinations}

You can use each modifier separately or combine them.

**Examples:**

Using the same modifier multiple times.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

Using multiple modifiers in a single query.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

## SETTINGS in SELECT Query {#settings-in-select-query}

You can specify the necessary settings right in the `SELECT` query. The setting value is applied only to this query and is reset to default or previous value after the query is executed.

Other ways to make settings see [here](/concepts/features/configuration/settings/overview).

For boolean settings set to true, you can use a shorthand syntax by omitting the value assignment. When only the setting name is specified, it is automatically set to `1` (true).

**Example**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```
)DOCS_MD",
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
        .description = R"DOCS_MD(
If `SELECT DISTINCT` is specified, only unique rows will remain in a query result. Thus, only a single row will remain out of all the sets of fully matching rows in the result.

You can specify the list of columns that must have unique values: `SELECT DISTINCT ON (column1, column2,...)`. If the columns are not specified, all of them are taken into consideration.

Consider the table:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Using `DISTINCT` without specifying columns:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Using `DISTINCT` with specified columns:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

## DISTINCT and ORDER BY {#distinct-and-order-by}

ClickHouse supports using the `DISTINCT` and `ORDER BY` clauses for different columns in one query. The `DISTINCT` clause is executed before the `ORDER BY` clause.

Consider the table:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Selecting data:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```
Selecting data with the different sorting direction:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Row `2, 4` was cut before sorting.

Take this implementation specificity into account when programming queries.

## Null Processing {#null-processing}

`DISTINCT` works with [NULL](/reference/syntax#null) as if `NULL` were a specific value, and `NULL==NULL`. In other words, in the `DISTINCT` results, different combinations with `NULL` occur only once. It differs from `NULL` processing in most other contexts.

## Alternatives {#alternatives}

It is possible to obtain the same result by applying [GROUP BY](/reference/statements/select/group-by) across the same set of values as specified as `SELECT` clause, without using any aggregate functions. But there are few differences from `GROUP BY` approach:

- `DISTINCT` can be applied together with `GROUP BY`.
- When [ORDER BY](/reference/statements/select/order-by) is omitted and [LIMIT](/reference/statements/select/limit) is defined, the query stops running immediately after the required number of different rows has been read.
- Data blocks are output as they are processed, without waiting for the entire query to finish running.
)DOCS_MD",
        .syntax = R"(
SELECT DISTINCT [ON (column1, column2, ...)] expr_list ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "ALL", "GROUP BY", "LIMIT BY"},
    });

    factory.registerStatement("ALL",
    {
        .description = R"DOCS_MD(
If there are multiple matching rows in a table, then `ALL` returns all of them. `SELECT ALL` is identical to `SELECT` without `DISTINCT`. If both `ALL` and `DISTINCT` are specified, then an exception will be thrown.

`ALL` can be specified inside aggregate functions, although it has no practical effect on the query's result.

For example:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

Is equivalent to:

```sql
SELECT sum(number) FROM numbers(10);
```
)DOCS_MD",
        .syntax = R"(
SELECT ALL expr_list ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "DISTINCT"},
    });

    factory.registerStatement("PREWHERE",
    {
        .description = R"DOCS_MD(
`PREWHERE` can make filtering more efficient by reducing the amount of data read. By default, ClickHouse applies this optimization, even when a query does not explicitly specify `PREWHERE`, by moving eligible conditions from [`WHERE`](/reference/statements/select/where) to `PREWHERE`. You can specify `PREWHERE` explicitly to control which conditions are applied at this stage.

With `PREWHERE`, ClickHouse first reads only the columns needed to evaluate the condition. It then reads the other columns required by the query only for blocks that contain at least one matching row. This can reduce the amount of data read when the condition uses fewer columns than the rest of the query and filters out many blocks.

## Controlling `PREWHERE` manually {#controlling-prewhere-manually}

Specify `PREWHERE` manually when a condition references a small number of columns and filters out many rows. This can reduce the amount of data read for the remaining columns.

A query can contain both `PREWHERE` and `WHERE`. In this case, `PREWHERE` is evaluated first.

Set [`optimize_move_to_prewhere`](/reference/settings/session-settings/optimize-move-to-prewhere#optimize_move_to_prewhere) to `0` to prevent ClickHouse from automatically moving conditions from `WHERE` to `PREWHERE`.

For queries with the [`FINAL`](/reference/statements/select/from#final-modifier) modifier, ClickHouse moves conditions from `WHERE` to `PREWHERE` only when both [`optimize_move_to_prewhere`](/reference/settings/session-settings/optimize-move-to-prewhere#optimize_move_to_prewhere) and [`optimize_move_to_prewhere_if_final`](/reference/settings/session-settings/optimize-move-to-prewhere#optimize_move_to_prewhere_if_final) are enabled.

<Note>
By default, `PREWHERE` is evaluated before `FINAL`, so `FROM ... FINAL` queries may produce unexpected results when `PREWHERE` references columns outside the table's `ORDER BY` key.
</Note>

## `PREWHERE` with `JOIN` {#prewhere-with-join}

A `PREWHERE` condition in a query with a [`JOIN`](/reference/statements/select/join) can directly reference columns from at most one table. ClickHouse applies the condition to that table's rows before they reach the join.

By contrast, a `WHERE` condition logically filters the joined result, although the optimizer may apply it before the join when doing so does not change the result. Using the same condition in `PREWHERE` and `WHERE` can therefore produce different results, particularly with outer joins.

The following example creates two tables to demonstrate this difference:

```sql
CREATE TABLE table_1
(
    `id` UInt32,
    `value` String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE table_2
(
    `id` UInt32,
    `value` String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table_1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO table_2 VALUES (1, 'x'), (2, 'y'), (3, 'z');
```

In the first query, `PREWHERE` filters `table_2` before the `LEFT JOIN`, so the row from `table_1` with `id = 1` remains unmatched:

```sql
SELECT
    table_1.id,
    table_1.value,
    table_2.value
FROM table_1
LEFT JOIN table_2 ON table_1.id = table_2.id
PREWHERE table_2.id >= 2
ORDER BY table_1.id;
```

```text
   ┌─id─┬─value─┬─table_2.value─┐
1. │  1 │ a     │               │
2. │  2 │ b     │ y             │
3. │  3 │ c     │ z             │
   └────┴───────┴───────────────┘
```

Using the same condition in `WHERE` filters the joined result, removing the row with `id = 1`:

```sql
SELECT
    table_1.id,
    table_1.value,
    table_2.value
FROM table_1
LEFT JOIN table_2 ON table_1.id = table_2.id
WHERE table_2.id >= 2
ORDER BY table_1.id;
```

```text
   ┌─id─┬─value─┬─table_2.value─┐
1. │  2 │ b     │ y             │
2. │  3 │ c     │ z             │
   └────┴───────┴───────────────┘
```

## Limitations {#limitations}

`PREWHERE` is only supported by tables from the [*MergeTree](/reference/engines/table-engines/mergetree-family/index) family.

## Example {#example}

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- Enable tracing to see which predicates are moved to PREWHERE.
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE
-- ClickHouse automatically moves B = 0 to PREWHERE, but this condition does not filter any rows because B is always 0.

-- Move the more selective C = 'x' predicate to PREWHERE.

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- The query with manually specified PREWHERE processes slightly less data: 158.89 MB instead of 168.89 MB.
```
)DOCS_MD",
        .syntax = R"(
SELECT ... PREWHERE expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "WHERE", "EXPLAIN"},
    });

    factory.registerStatement("WHERE",
    {
        .description = R"DOCS_MD(
The `WHERE` clause allows you to filter the data that comes from the[`FROM`](/reference/statements/select/from) clause of `SELECT`.

If there is a `WHERE` clause, it must be followed by an expression of type `UInt8`.
Rows where this expression evaluates to `0` are excluded from further transformations or the result.

The expression following the `WHERE` clause is often used with [comparison](/reference/operators/index#comparison-operators) and [logical operators](/reference/operators/index#operators-for-working-with-data-sets), or one of the many [regular functions](/reference/functions/regular-functions/overview).

The `WHERE` expression is evaluated on the ability to use indexes and partition pruning, if the underlying table engine supports that.

<Info>
**PREWHERE**

There is also a filtering optimization called [`PREWHERE`](/reference/statements/select/prewhere).
Prewhere is an optimization to apply filtering more efficiently.
It is enabled by default even if `PREWHERE` clause is not specified explicitly.
</Info>

## Testing for `NULL` {#testing-for-null}

If you need to test a value for [`NULL`](/reference/syntax#null), use:
- [`IS NULL`](/reference/operators/index#is_null) or [`isNull`](/reference/functions/regular-functions/functions-for-nulls#isNull)
- [`IS NOT NULL`](/reference/operators/index#is_not_null)   or [`isNotNull`](/reference/functions/regular-functions/functions-for-nulls#isNotNull)

An expression with `NULL` will otherwise never pass.

## Filtering data with logical operators {#filtering-data-with-logical-operators}

You can use the following [logical functions](/reference/functions/regular-functions/logical-functions#and) together with the `WHERE` clause for combining multiple conditions:

- [`and()`](/reference/functions/regular-functions/logical-functions#and) or `AND`
- [`not()`](/reference/functions/regular-functions/logical-functions#not) or `NOT`
- [`or()`](/reference/functions/regular-functions/logical-functions#or) or `NOT`
- [`xor()`](/reference/functions/regular-functions/logical-functions#xor)

## Using UInt8 columns as a condition {#using-uint8-columns-as-a-condition}

In ClickHouse, `UInt8` columns can be used directly as boolean conditions, where `0` is `false` and any non-zero value (typically `1`) is `true`.
An example of this is given in the section [below](#example-uint8-column-as-condition).

## Using comparison operators {#using-comparison-operators}

The following [comparison operators](/reference/operators/index#comparison-operators) can be used:

| Operator | Function | Description | Example |
|----------|----------|-------------|---------|
| `a = b` | `equals(a, b)` | Equal to | `price = 100` |
| `a == b` | `equals(a, b)` | Equal to (alternative syntax) | `price == 100` |
| `a != b` | `notEquals(a, b)` | Not equal to | `category != 'Electronics'` |
| `a <> b` | `notEquals(a, b)` | Not equal to (alternative syntax) | `category <> 'Electronics'` |
| `a < b` | `less(a, b)` | Less than | `price < 200` |
| `a <= b` | `lessOrEquals(a, b)` | Less than or equal to | `price <= 200` |
| `a > b` | `greater(a, b)` | Greater than | `price > 500` |
| `a >= b` | `greaterOrEquals(a, b)` | Greater than or equal to | `price >= 500` |
| `a LIKE s` | `like(a, b)` | Pattern matching (case-sensitive) | `name LIKE '%top%'` |
| `a NOT LIKE s` | `notLike(a, b)` | Pattern not matching (case-sensitive) | `name NOT LIKE '%top%'` |
| `a ILIKE s` | `ilike(a, b)` | Pattern matching (case-insensitive) | `name ILIKE '%LAPTOP%'` |
| `a BETWEEN b AND c` | `a >= b AND a <= c` | Range check (inclusive) | `price BETWEEN 100 AND 500` |
| `a NOT BETWEEN b AND c` | `a < b OR a > c` | Outside range check | `price NOT BETWEEN 100 AND 500` |

## Pattern matching and conditional expressions {#pattern-matching-and-conditional-expressions}

Beyond comparison operators, you can use pattern matching and conditional expressions in the `WHERE` clause.

| Feature     | Syntax                         | Case-Sensitive | Performance | Best For                       |
| ----------- | ------------------------------ | -------------- | ----------- | ------------------------------ |
| `LIKE`      | `col LIKE '%pattern%'`         | Yes            | Fast        | Exact case pattern matching    |
| `ILIKE`     | `col ILIKE '%pattern%'`        | No             | Slower      | Case-insensitive searching     |
| `if()`      | `if(cond, a, b)`               | N/A            | Fast        | Simple binary conditions       |
| `multiIf()` | `multiIf(c1, r1, c2, r2, def)` | N/A            | Fast        | Multiple conditions            |
| `CASE`      | `CASE WHEN ... THEN ... END`   | N/A            | Fast        | SQL-standard conditional logic |

See ["Pattern matching and conditional expressions"](#examples-pattern-matching-and-conditional-expressions) for usage examples.

## Expression with literals, columns or subqueries {#expressions-with-literals-columns-subqueries}

The expression following the `WHERE` clause can also include [literals](/reference/syntax#literals), columns or subqueries, which are nested `SELECT` statements that return values used in conditions.

| Type | Definition | Evaluation | Performance | Example |
|------|------------|------------|-------------|---------|
| **Literal** | Fixed constant value | Query write time | Fastest | `WHERE price > 100` |
| **Column** | Table data reference | Per row | Fast | `WHERE price > cost` |
| **Subquery** | Nested SELECT | Query execution time | Varies | `WHERE id IN (SELECT ...)` |

You can mix literals, columns, and subqueries in complex conditions:

```sql
-- Literal + Column
WHERE price > 100 AND category = 'Electronics'

-- Column + Subquery
WHERE price > (SELECT AVG(price) FROM products) AND in_stock = true

-- Literal + Column + Subquery
WHERE category = 'Electronics'
  AND price < 500
  AND id IN (SELECT product_id FROM bestsellers)

-- All three with logical operators
WHERE (price > 100 OR category IN (SELECT category FROM featured))
  AND in_stock = true
  AND name LIKE '%Special%'
```
## Examples {#examples}

### Testing for `NULL` {#examples-testing-for-null}

Queries with `NULL` values:

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE=MergeTree() ORDER BY x;
INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT * FROM t_null WHERE y IS NULL;
SELECT * FROM t_null WHERE y != 0;
```

```response
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

### Filtering data with logical operators {#example-filtering-with-logical-operators}

Given the following table and data:

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float32,
    category String,
    in_stock Bool
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO products VALUES
(1, 'Laptop', 999.99, 'Electronics', true),
(2, 'Mouse', 25.50, 'Electronics', true),
(3, 'Desk', 299.00, 'Furniture', false),
(4, 'Chair', 150.00, 'Furniture', true),
(5, 'Monitor', 350.00, 'Electronics', true),
(6, 'Lamp', 45.00, 'Furniture', false);
```

**1. `AND` - both conditions must be true:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND price < 500;
```

```response
   ┌─id─┬─name────┬─price─┬─category────┬─in_stock─┐
1. │  2 │ Mouse   │  25.5 │ Electronics │ true     │
2. │  5 │ Monitor │   350 │ Electronics │ true     │
   └────┴─────────┴───────┴─────────────┴──────────┘
```

**2. `OR` - at least one condition must be true:**

```sql
SELECT * FROM products
WHERE category = 'Furniture' OR price > 500;
```

```response
   ┌─id─┬─name───┬──price─┬─category────┬─in_stock─┐
1. │  1 │ Laptop │ 999.99 │ Electronics │ true     │
2. │  3 │ Desk   │    299 │ Furniture   │ false    │
3. │  4 │ Chair  │    150 │ Furniture   │ true     │
4. │  6 │ Lamp   │     45 │ Furniture   │ false    │
   └────┴────────┴────────┴─────────────┴──────────┘
```

**3. `NOT` - Negates a condition:**

```sql
SELECT * FROM products
WHERE NOT in_stock;
```

```response
   ┌─id─┬─name─┬─price─┬─category──┬─in_stock─┐
1. │  3 │ Desk │   299 │ Furniture │ false    │
2. │  6 │ Lamp │    45 │ Furniture │ false    │
   └────┴──────┴───────┴───────────┴──────────┘
```

**4. `XOR` - Exactly one condition must be true (not both):**

```sql
SELECT *
FROM products
WHERE xor(price > 200, category = 'Electronics')
```

```response
   ┌─id─┬─name──┬─price─┬─category────┬─in_stock─┐
1. │  2 │ Mouse │  25.5 │ Electronics │ true     │
2. │  3 │ Desk  │   299 │ Furniture   │ false    │
   └────┴───────┴───────┴─────────────┴──────────┘
```

**5. Combining multiple operators:**

```sql
SELECT * FROM products
WHERE (category = 'Electronics' OR category = 'Furniture')
  AND in_stock = true
  AND price < 400;
```

```response
   ┌─id─┬─name────┬─price─┬─category────┬─in_stock─┐
1. │  2 │ Mouse   │  25.5 │ Electronics │ true     │
2. │  4 │ Chair   │   150 │ Furniture   │ true     │
3. │  5 │ Monitor │   350 │ Electronics │ true     │
   └────┴─────────┴───────┴─────────────┴──────────┘
```

**6. Using function syntax:**

```sql
SELECT * FROM products
WHERE and(or(category = 'Electronics', price > 100), in_stock);
```

```response
   ┌─id─┬─name────┬──price─┬─category────┬─in_stock─┐
1. │  1 │ Laptop  │ 999.99 │ Electronics │ true     │
2. │  2 │ Mouse   │   25.5 │ Electronics │ true     │
3. │  4 │ Chair   │    150 │ Furniture   │ true     │
4. │  5 │ Monitor │    350 │ Electronics │ true     │
   └────┴─────────┴────────┴─────────────┴──────────┘
```

The SQL keyword syntax (`AND`, `OR`, `NOT`, `XOR`) is generally more readable, but the function syntax can be useful in complex expressions or when building dynamic queries.

### Using UInt8 columns as a condition {#example-uint8-column-as-condition}

Taking the table from a [previous example](#example-filtering-with-logical-operators), you can use a column name directly as a condition:

```sql
SELECT * FROM products
WHERE in_stock
```

```response
   ┌─id─┬─name────┬──price─┬─category────┬─in_stock─┐
1. │  1 │ Laptop  │ 999.99 │ Electronics │ true     │
2. │  2 │ Mouse   │   25.5 │ Electronics │ true     │
3. │  4 │ Chair   │    150 │ Furniture   │ true     │
4. │  5 │ Monitor │    350 │ Electronics │ true     │
   └────┴─────────┴────────┴─────────────┴──────────┘
```

### Using comparison operators {#example-using-comparison-operators}

The examples below use the table and data from the [example](#example-filtering-with-logical-operators) above. Results are omitted for sake of brevity.

**1. Explicit equality with true (`= 1` or `= true`):**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. Explicit equality with false (`= 0` or `= false`):**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. Inequality (`!= 0` or `!= false`):**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. Greater than:**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. Less than or equal:**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. Combining with other conditions:**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. Using the `IN` operator:**

In the example below `(1, true)` is a [tuple](/reference/data-types/tuple).

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

You can also use an [array](/reference/data-types/array) to do this:

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. Mixing comparison styles:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

### Pattern matching and conditional expressions {#examples-pattern-matching-and-conditional-expressions}

The examples below use the table and data from the [example](#example-filtering-with-logical-operators) above. Results are omitted for sake of brevity.

#### LIKE examples {#like-examples}

```sql
-- Find products with 'o' in the name
SELECT * FROM products WHERE name LIKE '%o%';
-- Result: Laptop, Monitor

-- Find products starting with 'L'
SELECT * FROM products WHERE name LIKE 'L%';
-- Result: Laptop, Lamp

-- Find products with exactly 4 characters
SELECT * FROM products WHERE name LIKE '____';
-- Result: Desk, Lamp
```

#### ILIKE examples {#ilike-examples}

```sql
-- Case-insensitive search for 'LAPTOP'
SELECT * FROM products WHERE name ILIKE '%laptop%';
-- Result: Laptop

-- Case-insensitive prefix match
SELECT * FROM products WHERE name ILIKE 'l%';
-- Result: Laptop, Lamp
```

#### IF examples {#if-examples}

```sql
-- Different price thresholds by category
SELECT * FROM products
WHERE if(category = 'Electronics', price < 500, price < 200);
-- Result: Mouse, Chair, Monitor
-- (Electronics under $500 OR Furniture under $200)

-- Filter based on stock status
SELECT * FROM products
WHERE if(in_stock, price > 100, true);
-- Result: Laptop, Chair, Monitor, Desk, Lamp
-- (In stock items over $100 OR all out-of-stock items)
```

#### multiIf examples {#multiif-examples}

```sql
-- Multiple category-based conditions
SELECT * FROM products
WHERE multiIf(
    category = 'Electronics', price < 600,
    category = 'Furniture', in_stock = true,
    false
);
-- Result: Mouse, Monitor, Chair
-- (Electronics < $600 OR in-stock Furniture)

-- Tiered filtering
SELECT * FROM products
WHERE multiIf(
    price > 500, category = 'Electronics',
    price > 100, in_stock = true,
    true
);
-- Result: Laptop, Chair, Monitor, Lamp
```

#### CASE examples {#case-examples}

**Simple CASE:**

```sql
-- Different rules per category
SELECT * FROM products
WHERE CASE category
    WHEN 'Electronics' THEN price < 400
    WHEN 'Furniture' THEN in_stock = true
    ELSE false
END;
-- Result: Mouse, Monitor, Chair
```

**Searched CASE:**

```sql
-- Price-based tiered logic
SELECT * FROM products
WHERE CASE
    WHEN price > 500 THEN in_stock = true
    WHEN price > 100 THEN category = 'Electronics'
    ELSE true
END;
-- Result: Laptop, Monitor, Mouse, Lamp
```
)DOCS_MD",
        .syntax = R"(
SELECT ... WHERE expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "PREWHERE", "HAVING", "QUALIFY"},
    });

    factory.registerStatement("GROUP BY",
    {
        .description = R"DOCS_MD(
`GROUP BY` clause switches the `SELECT` query into an aggregation mode, which works as follows:

- `GROUP BY` clause contains a list of expressions (or a single expression, which is considered to be the list of length one). This list acts as a "grouping key", while each individual expression will be referred to as a "key expression".
- All the expressions in the [SELECT](/reference/statements/select/index), [HAVING](/reference/statements/select/having), and [ORDER BY](/reference/statements/select/order-by) clauses **must** be calculated based on key expressions **or** on [aggregate functions](/reference/functions/aggregate-functions/index) over non-key expressions (including plain columns). In other words, each column selected from the table must be used either in a key expression or inside an aggregate function, but not both.
- Result of aggregating `SELECT` query will contain as many rows as there were unique values of "grouping key" in source table. Usually, this significantly reduces the row count, often by orders of magnitude, but not necessarily: row count stays the same if all "grouping key" values were distinct.

When you want to group data in the table by column numbers instead of column names, enable the setting [enable_positional_arguments](/reference/settings/session-settings/enable-positional-arguments#enable_positional_arguments).

<Note>
There's an additional way to run aggregation over a table. If a query contains table columns only inside aggregate functions, the `GROUP BY clause` can be omitted, and aggregation by an empty set of keys is assumed. Such queries always return exactly one row.
</Note>

## NULL Processing {#null-processing}

For grouping, ClickHouse interprets [NULL](/reference/syntax#null) as a value, and `NULL==NULL`. It differs from `NULL` processing in most other contexts.

Here's an example to show what this means.

Assume you have this table:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

The query `SELECT sum(x), y FROM t_null_big GROUP BY y` results in:

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

You can see that `GROUP BY` for `y = NULL` summed up `x`, as if `NULL` is this value.

If you pass several keys to `GROUP BY`, the result will give you all the combinations of the selection, as if `NULL` were a specific value.

## ROLLUP Modifier {#rollup-modifier}

`ROLLUP` modifier is used to calculate subtotals for the key expressions, based on their order in the `GROUP BY` list. The subtotals rows are added after the result table.

The subtotals are calculated in the reverse order: at first subtotals are calculated for the last key expression in the list, then for the previous one, and so on up to the first key expression.

In the subtotals rows the values of already "grouped" key expressions are set to `0` or empty line.

<Note>
Mind that [HAVING](/reference/statements/select/having) clause can affect the subtotals results.
</Note>

**Example**

Consider the table t:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```
As `GROUP BY` section has three key expressions, the result contains four tables with subtotals "rolled up" from right to left:

- `GROUP BY year, month, day`;
- `GROUP BY year, month` (and `day` column is filled with zeros);
- `GROUP BY year` (now `month, day` columns are both filled with zeros);
- and totals (and all three key expression columns are zeros).

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```
The same query also can be written using `WITH` keyword.
```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**See also**

- [group_by_use_nulls](/reference/settings/session-settings/group-by#group_by_use_nulls) setting for SQL standard compatibility.

## CUBE Modifier {#cube-modifier}

`CUBE` modifier is used to calculate subtotals for every combination of the key expressions in the `GROUP BY` list. The subtotals rows are added after the result table.

In the subtotals rows the values of all "grouped" key expressions are set to `0` or empty line.

<Note>
Mind that [HAVING](/reference/statements/select/having) clause can affect the subtotals results.
</Note>

**Example**

Consider the table t:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

As `GROUP BY` section has three key expressions, the result contains eight tables with subtotals for all key expression combinations:

- `GROUP BY year, month, day`
- `GROUP BY year, month`
- `GROUP BY year, day`
- `GROUP BY year`
- `GROUP BY month, day`
- `GROUP BY month`
- `GROUP BY day`
- and totals.

Columns, excluded from `GROUP BY`, are filled with zeros.

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```
The same query also can be written using `WITH` keyword.
```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**See also**

- [group_by_use_nulls](/reference/settings/session-settings/group-by#group_by_use_nulls) setting for SQL standard compatibility.

## WITH TOTALS Modifier {#with-totals-modifier}

If the `WITH TOTALS` modifier is specified, another row will be calculated. This row will have key columns containing default values (zeros or empty lines), and columns of aggregate functions with the values calculated across all the rows (the "total" values).

This extra row is only produced in `JSON*`, `TabSeparated*`, and `Pretty*` formats, separately from the other rows:

- In `XML` and `JSON*` formats, this row is output as a separate 'totals' field.
- In `TabSeparated*`, `CSV*` and `Vertical` formats, the row comes after the main result, preceded by an empty row (after the other data).
- In `Pretty*` formats, the row is output as a separate table after the main result.
- In `Template` format, the row is output according to specified template.
- In the other formats it is not available.

<Note>
totals is output in the results of `SELECT` queries, and is not output in `INSERT INTO ... SELECT`.
</Note>

`WITH TOTALS` can be run in different ways when [HAVING](/reference/statements/select/having) is present. The behavior depends on the `totals_mode` setting.

### Configuring Totals Processing {#configuring-totals-processing}

By default, `totals_mode = 'before_having'`. In this case, 'totals' is calculated across all rows, including the ones that do not pass through HAVING and `max_rows_to_group_by`.

The other alternatives include only the rows that pass through HAVING in 'totals', and behave differently with the setting `max_rows_to_group_by` and `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. In other words, 'totals' will have less than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_inclusive` – Include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. In other words, 'totals' will have more than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. Otherwise, do not include them.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

If `max_rows_to_group_by` and `group_by_overflow_mode = 'any'` are not used, all variations of `after_having` are the same, and you can use any of them (for example, `after_having_auto`).

You can use `WITH TOTALS` in subqueries, including subqueries in the [JOIN](/reference/statements/select/join) clause (in this case, the respective total values are combined).

## GROUP BY ALL {#group-by-all}

`GROUP BY ALL` is equivalent to listing all the SELECT-ed expressions that are not aggregate functions.

For example:

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

is the same as

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

For a special case that if there is a function having both aggregate functions and other fields as its arguments, the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.

For example:

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

is the same as

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

## Examples {#examples}

Example:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

As opposed to MySQL (and conforming to standard SQL), you can't get some value of some column that is not in a key or aggregate function (except constant expressions). To work around this, you can use the 'any' aggregate function (get the first encountered value) or 'min/max'.

Example:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

For every different key value encountered, `GROUP BY` calculates a set of aggregate function values.

## GROUPING SETS modifier {#grouping-sets-modifier}

This is the most general modifier.
This modifier allows manually specifying several aggregation key sets (grouping sets).
Aggregation is performed separately for each grouping set, and after that, all results are combined.
If a column is not presented in a grouping set, it's filled with a default value.

In other words, modifiers described above can be represented via `GROUPING SETS`.
Despite the fact that queries with `ROLLUP`, `CUBE` and `GROUPING SETS` modifiers are syntactically equal, they may perform differently.
When `GROUPING SETS` try to execute everything in parallel, `ROLLUP` and `CUBE` are executing the final merging of the aggregates in a single thread.

In the situation when source columns contain default values, it might be hard to distinguish if a row is a part of the aggregation which uses those columns as keys or not.
To solve this problem `GROUPING` function must be used.

**Example**

The following two queries are equivalent.

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**See also**

- [group_by_use_nulls](/reference/settings/session-settings/group-by#group_by_use_nulls) setting for SQL standard compatibility.

## Implementation Details {#implementation-details}

Aggregation is one of the most important features of a column-oriented DBMS, and thus it's implementation is one of the most heavily optimized parts of ClickHouse. By default, aggregation is done in memory using a hash-table. It has 40+ specializations that are chosen automatically depending on "grouping key" data types.

### GROUP BY Optimization Depending on Table Sorting Key {#group-by-optimization-depending-on-table-sorting-key}

The aggregation can be performed more effectively, if a table is sorted by some key, and `GROUP BY` expression contains at least prefix of sorting key or injective functions. In this case when a new key is read from table, the in-between result of aggregation can be finalized and sent to client. This behaviour is switched on by the [optimize_aggregation_in_order](/reference/settings/session-settings/optimize-aggregation-in-order#optimize_aggregation_in_order) setting. Such optimization reduces memory usage during aggregation, but in some cases may slow down the query execution.

### GROUP BY in External Memory {#group-by-in-external-memory}

You can enable dumping temporary data to the disk to restrict memory usage during `GROUP BY`.
The [max_bytes_before_external_group_by](/reference/settings/session-settings/max-bytes#max_bytes_before_external_group_by) setting determines the threshold RAM consumption for dumping `GROUP BY` temporary data to the file system. If set to 0 (the default), it is disabled.
Alternatively, you can set [max_bytes_ratio_before_external_group_by](/reference/settings/session-settings/max-bytes#max_bytes_ratio_before_external_group_by), which allows to use `GROUP BY` in external memory only once the query reaches certain threshold of used memory.

When using `max_bytes_before_external_group_by`, we recommend that you set `max_memory_usage` about twice as high (or `max_bytes_ratio_before_external_group_by=0.5`). This is necessary because there are two stages to aggregation: reading the data and forming intermediate data (1) and merging the intermediate data (2). Dumping data to the file system can only occur during stage 1. If the temporary data wasn't dumped, then stage 2 might require up to the same amount of memory as in stage 1.

For example, if [max_memory_usage](/reference/settings/session-settings/max-memory-usage#max_memory_usage) was set to 10000000000 and you want to use external aggregation, it makes sense to set `max_bytes_before_external_group_by` to 10000000000, and `max_memory_usage` to 20000000000. When external aggregation is triggered (if there was at least one dump of temporary data), maximum consumption of RAM is only slightly more than `max_bytes_before_external_group_by`.

With distributed query processing, external aggregation is performed on remote servers. In order for the requester server to use only a small amount of RAM, set `distributed_aggregation_memory_efficient` to 1.

When merging data flushed to the disk, as well as when merging results from remote servers when the `distributed_aggregation_memory_efficient` setting is enabled, consumes up to `1/256 * the_number_of_threads` from the total amount of RAM.

When external aggregation is enabled, if there was less than `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

If you have an [ORDER BY](/reference/statements/select/order-by) with a [LIMIT](/reference/statements/select/limit) after `GROUP BY`, then the amount of used RAM depends on the amount of data in `LIMIT`, not in the whole table. But if the `ORDER BY` does not have `LIMIT`, do not forget to enable external sorting (`max_bytes_before_external_sort`).
)DOCS_MD",
        .syntax = R"(
SELECT ... GROUP BY expr_list [WITH ROLLUP | WITH CUBE] [WITH TOTALS] ...
SELECT ... GROUP BY ROLLUP(expr_list) | CUBE(expr_list) | GROUPING SETS (...) ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "HAVING", "DISTINCT", "ORDER BY"},
    });

    factory.registerStatement("HAVING",
    {
        .description = R"DOCS_MD(
Allows filtering the aggregation results produced by [GROUP BY](/reference/statements/select/group-by). It is similar to the [WHERE](/reference/statements/select/where) clause, but the difference is that `WHERE` is performed before aggregation, while `HAVING` is performed after it.

It is possible to reference aggregation results from `SELECT` clause in `HAVING` clause by their alias. Alternatively, `HAVING` clause can filter on results of additional aggregates that are not returned in query results.

## Example {#example}
If you have a `sales` table as follows:
```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

You can query it like so:
```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```
This will list sales people with greater than 10,000 in total sales in their region.
## Limitations {#limitations}

`HAVING` can't be used if aggregation is not performed. Use `WHERE` instead.
)DOCS_MD",
        .syntax = R"(
SELECT ... GROUP BY ... HAVING expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "GROUP BY", "WHERE", "QUALIFY"},
    });

    factory.registerStatement("QUALIFY",
    {
        .description = R"DOCS_MD(
Allows filtering window functions results. It is similar to the [WHERE](/reference/statements/select/where) clause, but the difference is that `WHERE` is performed before window functions evaluation, while `QUALIFY` is performed after it.

It is possible to reference window functions results from `SELECT` clause in `QUALIFY` clause by their alias. Alternatively, `QUALIFY` clause can filter on results of additional window functions that are not returned in query results.

## Limitations {#limitations}

`QUALIFY` can't be used if there are no window functions to evaluate. Use `WHERE` instead.

## Examples {#examples}

Example:

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```
)DOCS_MD",
        .syntax = R"(
SELECT ... QUALIFY expr ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "WHERE", "HAVING"},
    });

    factory.registerStatement("ORDER BY",
    {
        .description = R"DOCS_MD(
The `ORDER BY` clause contains

- a list of expressions, e.g. `ORDER BY visits, search_phrase`,
- a list of numbers referring to columns in the `SELECT` clause, e.g. `ORDER BY 2, 1`, or
- `ALL` which means all columns of the `SELECT` clause, e.g. `ORDER BY ALL`.

To disable sorting by column numbers, set setting [enable_positional_arguments](/reference/settings/session-settings/enable-positional-arguments#enable_positional_arguments) = 0.
To disable sorting by `ALL`, set setting [enable_order_by_all](/reference/settings/session-settings/enable#enable_order_by_all) = 0.

The `ORDER BY` clause can be attributed by a `DESC` (descending) or `ASC` (ascending) modifier which determines the sorting direction.
Unless an explicit sort order is specified, `ASC` is used by default.
The sorting direction applies to a single expression, not to the entire list, e.g. `ORDER BY Visits DESC, SearchPhrase`.
Also, sorting is performed case-sensitively.

Rows with identical values for a sort expressions are returned in an arbitrary and non-deterministic order.
If the `ORDER BY` clause is omitted in a `SELECT` statement, the row order is also arbitrary and non-deterministic.

## Sorting of Special Values {#sorting-of-special-values}

There are two approaches to `NaN` and `NULL` sorting order:

- By default or with the `NULLS LAST` modifier: first the values, then `NaN`, then `NULL`.
- With the `NULLS FIRST` modifier: first `NULL`, then `NaN`, then other values.

### Example {#example}

For the table

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Run the query `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` to get:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

When floating point numbers are sorted, NaNs are separate from the other values. Regardless of the sorting order, NaNs come at the end. In other words, for ascending sorting they are placed as if they are larger than all the other numbers, while for descending sorting they are placed as if they are smaller than the rest.

## Collation Support {#collation-support}

For sorting by [String](/reference/data-types/string) values, you can specify collation (comparison). Example: `ORDER BY SearchPhrase COLLATE 'tr'` - for sorting by keyword in ascending order, using the Turkish alphabet, case insensitive, assuming that strings are UTF-8 encoded. `COLLATE` can be specified or not for each expression in ORDER BY independently. If `ASC` or `DESC` is specified, `COLLATE` is specified after it. When using `COLLATE`, sorting is always case-insensitive.

Collate is supported in [LowCardinality](/reference/data-types/lowcardinality), [Nullable](/reference/data-types/nullable), [Array](/reference/data-types/array) and [Tuple](/reference/data-types/tuple).

We only recommend using `COLLATE` for final sorting of a small number of rows, since sorting with `COLLATE` is less efficient than normal sorting by bytes.

## Collation Examples {#collation-examples}

Example only with [String](/reference/data-types/string) values:

Input table:

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

Example with [Nullable](/reference/data-types/nullable):

Input table:

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │ ABC  │
│ 4 │ 123a │
│ 5 │ abc  │
│ 6 │ ᴺᵁᴸᴸ │
│ 7 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 4 │ 123a │
│ 5 │ abc  │
│ 3 │ ABC  │
│ 1 │ bca  │
│ 7 │ BCA  │
│ 6 │ ᴺᵁᴸᴸ │
│ 2 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Example with [Array](/reference/data-types/array):

Input table:

```text
┌─x─┬─s─────────────┐
│ 1 │ ['Z']         │
│ 2 │ ['z']         │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 7 │ ['']          │
└───┴───────────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s─────────────┐
│ 7 │ ['']          │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 2 │ ['z']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 1 │ ['Z']         │
└───┴───────────────┘
```

Example with [LowCardinality](/reference/data-types/lowcardinality) string:

Input table:

```response
┌─x─┬─s───┐
│ 1 │ Z   │
│ 2 │ z   │
│ 3 │ a   │
│ 4 │ A   │
│ 5 │ za  │
│ 6 │ zaa │
│ 7 │     │
└───┴─────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───┐
│ 7 │     │
│ 3 │ a   │
│ 4 │ A   │
│ 2 │ z   │
│ 1 │ Z   │
│ 5 │ za  │
│ 6 │ zaa │
└───┴─────┘
```

Example with [Tuple](/reference/data-types/tuple):

```response title="Response"
┌─x─┬─s───────┐
│ 1 │ (1,'Z') │
│ 2 │ (1,'z') │
│ 3 │ (1,'a') │
│ 4 │ (2,'z') │
│ 5 │ (1,'A') │
│ 6 │ (2,'Z') │
│ 7 │ (2,'A') │
└───┴─────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───────┐
│ 3 │ (1,'a') │
│ 5 │ (1,'A') │
│ 2 │ (1,'z') │
│ 1 │ (1,'Z') │
│ 7 │ (2,'A') │
│ 4 │ (2,'z') │
│ 6 │ (2,'Z') │
└───┴─────────┘
```

## Implementation Details {#implementation-details}

Less RAM is used if a small enough [LIMIT](/reference/statements/select/limit) is specified in addition to `ORDER BY`. Otherwise, the amount of memory spent is proportional to the volume of data for sorting. For distributed query processing, if [GROUP BY](/reference/statements/select/group-by) is omitted, sorting is partially done on remote servers, and the results are merged on the requestor server. This means that for distributed sorting, the volume of data to sort can be greater than the amount of memory on a single server.

If there is not enough RAM, it is possible to perform sorting in external memory (creating temporary files on a disk). Use the setting `max_bytes_before_external_sort` for this purpose. If it is set to 0 (the default), external sorting is disabled. If it is enabled, when the volume of data to sort reaches the specified number of bytes, the collected data is sorted and dumped into a temporary file. After all data is read, all the sorted files are merged and the results are output. Files are written to the `/var/lib/clickhouse/tmp/` directory in the config (by default, but you can use the `tmp_path` parameter to change this setting). You can also use spilling to disk only if query exceeds memory limits, i.e. `max_bytes_ratio_before_external_sort=0.6` will enable spilling to disk only once the query hits `60%` memory limit (user/sever).

Running a query may use more memory than `max_bytes_before_external_sort`. For this reason, this setting must have a value significantly smaller than `max_memory_usage`. As an example, if your server has 128 GB of RAM and you need to run a single query, set `max_memory_usage` to 100 GB, and `max_bytes_before_external_sort` to 80 GB.

External sorting works much less effectively than sorting in RAM.

## Optimization of Data Reading {#optimization-of-data-reading}

 If `ORDER BY` expression has a prefix that coincides with the table sorting key, you can optimize the query by using the [optimize_read_in_order](/reference/settings/session-settings/optimize#optimize_read_in_order) setting.

 When the `optimize_read_in_order` setting is enabled, the ClickHouse server uses the table index and reads the data in order of the `ORDER BY` key. This allows to avoid reading all data in case of specified [LIMIT](/reference/statements/select/limit). So queries on big data with small limit are processed faster.

Optimization works with both `ASC` and `DESC` and does not work together with the [GROUP BY](/reference/statements/select/group-by) clause. With the [FINAL](/reference/statements/select/from#final-modifier) modifier, the optimization works in the direct order of the sorting key, and for [ReplacingMergeTree](/reference/engines/table-engines/mergetree-family/replacingmergetree) tables also in the reverse order, controlled by the `optimize_read_in_reverse_order_final` setting.

When the `optimize_read_in_order` setting is disabled, the ClickHouse server does not use the table index while processing `SELECT` queries.

Consider disabling `optimize_read_in_order` manually, when running queries that have `ORDER BY` clause, large `LIMIT` and [WHERE](/reference/statements/select/where) condition that requires to read huge amount of records before queried data is found.

Optimization is supported in the following table engines:

- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) (including [materialized views](/reference/statements/create/view#materialized-view)),
- [Merge](/reference/engines/table-engines/special/merge),
- [Buffer](/reference/engines/table-engines/special/buffer)

In `MaterializedView`-engine tables the optimization works with views like `SELECT ... FROM merge_tree_table ORDER BY pk`. But it is not supported in the queries like `SELECT ... FROM view ORDER BY pk` if the view query does not have the `ORDER BY` clause.

## ORDER BY Expr WITH FILL Modifier {#order-by-expr-with-fill-modifier}

This modifier also can be combined with [LIMIT ... WITH TIES modifier](/reference/statements/select/limit#limit--with-ties-modifier).

`WITH FILL` modifier can be set after `ORDER BY expr` with optional `FROM expr`, `TO expr` and `STEP expr` parameters.
All missed values of `expr` column will be filled sequentially and other columns will be filled as defaults.

To fill multiple columns, add `WITH FILL` modifier with optional parameters after each field name in `ORDER BY` section.

```sql title="Query"
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

`WITH FILL` can be applied for fields with Numeric (all kinds of float, decimal, int) or Date/DateTime types. When applied for `String` fields, missed values are filled with empty strings.
When `FROM const_expr` not defined sequence of filling use minimal `expr` field value from `ORDER BY`.
When `TO const_expr` not defined sequence of filling use maximum `expr` field value from `ORDER BY`.
When `STEP const_numeric_expr` defined then `const_numeric_expr` interprets `as is` for numeric types, as `days` for Date type, as `seconds` for DateTime type. It also supports [INTERVAL](/reference/data-types/special-data-types/interval) data type representing time and date intervals.
When `STEP const_numeric_expr` omitted then sequence of filling use `1.0` for numeric type, `1 day` for Date type and `1 second` for DateTime type.
When `STALENESS const_numeric_expr` is defined, the query will generate rows until the difference from the previous row in the original data exceeds `const_numeric_expr`.
`INTERPOLATE` can be applied to columns not participating in `ORDER BY WITH FILL`. Such columns are filled based on previous fields values by applying `expr`. If `expr` is not present will repeat previous value. Omitted list will result in including all allowed columns.

Example of a query without `WITH FILL`:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

```text title="Response"
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

Same query after applying `WITH FILL` modifier:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┐
│   0 │          │
│ 0.5 │          │
│   1 │ original │
│ 1.5 │          │
│   2 │          │
│ 2.5 │          │
│   3 │          │
│ 3.5 │          │
│   4 │ original │
│ 4.5 │          │
│   5 │          │
│ 5.5 │          │
│   7 │ original │
└─────┴──────────┘
```

For the case with multiple fields `ORDER BY field2 WITH FILL, field1 WITH FILL` order of filling will follow the order of fields in the `ORDER BY` clause.

Example:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL,
    d1 WITH FILL STEP 5;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-01 │ 1970-01-03 │          │
│ 1970-01-01 │ 1970-01-04 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-01-01 │ 1970-01-06 │          │
│ 1970-01-01 │ 1970-01-07 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

Field `d1` does not fill in and use the default value cause we do not have repeated values for `d2` value, and the sequence for `d1` can't be properly calculated.

The following query with the changed field in `ORDER BY`:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

The following query uses the `INTERVAL` data type of 1 day for each data filled on column `d1`:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP INTERVAL 1 DAY,
    d2 WITH FILL;
```

```response title="Response"
┌─────────d1─┬─────────d2─┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-12 │ 1970-01-01 │          │
│ 1970-01-13 │ 1970-01-01 │          │
│ 1970-01-14 │ 1970-01-01 │          │
│ 1970-01-15 │ 1970-01-01 │          │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-17 │ 1970-01-01 │          │
│ 1970-01-18 │ 1970-01-01 │          │
│ 1970-01-19 │ 1970-01-01 │          │
│ 1970-01-20 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-22 │ 1970-01-01 │          │
│ 1970-01-23 │ 1970-01-01 │          │
│ 1970-01-24 │ 1970-01-01 │          │
│ 1970-01-25 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-27 │ 1970-01-01 │          │
│ 1970-01-28 │ 1970-01-01 │          │
│ 1970-01-29 │ 1970-01-01 │          │
│ 1970-01-30 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-01 │ 1970-01-01 │          │
│ 1970-02-02 │ 1970-01-01 │          │
│ 1970-02-03 │ 1970-01-01 │          │
│ 1970-02-04 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-06 │ 1970-01-01 │          │
│ 1970-02-07 │ 1970-01-01 │          │
│ 1970-02-08 │ 1970-01-01 │          │
│ 1970-02-09 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-11 │ 1970-01-01 │          │
│ 1970-02-12 │ 1970-01-01 │          │
│ 1970-02-13 │ 1970-01-01 │          │
│ 1970-02-14 │ 1970-01-01 │          │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-16 │ 1970-01-01 │          │
│ 1970-02-17 │ 1970-01-01 │          │
│ 1970-02-18 │ 1970-01-01 │          │
│ 1970-02-19 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-21 │ 1970-01-01 │          │
│ 1970-02-22 │ 1970-01-01 │          │
│ 1970-02-23 │ 1970-01-01 │          │
│ 1970-02-24 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-02-26 │ 1970-01-01 │          │
│ 1970-02-27 │ 1970-01-01 │          │
│ 1970-02-28 │ 1970-01-01 │          │
│ 1970-03-01 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-03 │ 1970-01-01 │          │
│ 1970-03-04 │ 1970-01-01 │          │
│ 1970-03-05 │ 1970-01-01 │          │
│ 1970-03-06 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-08 │ 1970-01-01 │          │
│ 1970-03-09 │ 1970-01-01 │          │
│ 1970-03-10 │ 1970-01-01 │          │
│ 1970-03-11 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

Example of a query without `STALENESS`:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   3 │     0 │          │
 5. │   4 │     0 │          │
 6. │   5 │    25 │ original │
 7. │   6 │     0 │          │
 8. │   7 │     0 │          │
 9. │   8 │     0 │          │
10. │   9 │     0 │          │
11. │  10 │    50 │ original │
12. │  11 │     0 │          │
13. │  12 │     0 │          │
14. │  13 │     0 │          │
15. │  14 │     0 │          │
16. │  15 │    75 │ original │
    └─────┴───────┴──────────┘
```

Same query after applying `STALENESS 3`:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   5 │    25 │ original │
 5. │   6 │     0 │          │
 6. │   7 │     0 │          │
 7. │  10 │    50 │ original │
 8. │  11 │     0 │          │
 9. │  12 │     0 │          │
10. │  15 │    75 │ original │
11. │  16 │     0 │          │
12. │  17 │     0 │          │
    └─────┴───────┴──────────┘
```

Example of a query without `INTERPOLATE`:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     0 │
│   2 │          │     0 │
│ 2.5 │          │     0 │
│   3 │          │     0 │
│ 3.5 │          │     0 │
│   4 │ original │     4 │
│ 4.5 │          │     0 │
│   5 │          │     0 │
│ 5.5 │          │     0 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

Same query after applying `INTERPOLATE`:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     2 │
│   2 │          │     3 │
│ 2.5 │          │     4 │
│   3 │          │     5 │
│ 3.5 │          │     6 │
│   4 │ original │     4 │
│ 4.5 │          │     5 │
│   5 │          │     6 │
│ 5.5 │          │     7 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

## Filling grouped by sorting prefix {#filling-grouped-by-sorting-prefix}

It can be useful to fill rows which have the same values in particular columns independently, - a good example is filling missing values in time series.
Assume there is the following time series table:
```sql
CREATE TABLE timeseries
(
    `sensor_id` UInt64,
    `timestamp` DateTime64(3, 'UTC'),
    `value` Float64
)
ENGINE = Memory;

SELECT * FROM timeseries;

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```
And we'd like to fill missing values for each sensor independently with 1 second interval.
The way to achieve it is to use `sensor_id` column as sorting prefix for filling column `timestamp`:
```sql
SELECT *
FROM timeseries
ORDER BY
    sensor_id,
    timestamp WITH FILL
INTERPOLATE ( value AS 9999 )

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       234 │ 2021-12-01 00:00:04.000 │  9999 │
│       234 │ 2021-12-01 00:00:05.000 │  9999 │
│       234 │ 2021-12-01 00:00:06.000 │  9999 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       432 │ 2021-12-01 00:00:02.000 │  9999 │
│       432 │ 2021-12-01 00:00:03.000 │  9999 │
│       432 │ 2021-12-01 00:00:04.000 │  9999 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```
Here, the `value` column was interpolated with `9999` just to make filled rows more noticeable.
This behavior is controlled by setting `use_with_fill_by_sorting_prefix` (enabled by default)

## Related content {#related-content}

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
)DOCS_MD",
        .syntax = R"(
SELECT ... ORDER BY expr [ASC | DESC] [NULLS FIRST | NULLS LAST] [COLLATE 'locale'] [, ...]
    [WITH FILL [FROM expr] [TO expr] [STEP expr] [STALENESS expr]] [INTERPOLATE [(expr_list)]] ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "LIMIT", "GROUP BY", "ALTER TABLE ... MODIFY ORDER BY"},
    });

    factory.registerStatement("LIMIT",
    {
        .description = R"DOCS_MD(
The `LIMIT` clause controls how many rows are returned from your query results.

## Basic syntax {#basic-syntax}

**Select first rows:**

```sql
LIMIT m
```

Returns the first `m` rows from the result, or all records when there are fewer than `m`.

**Alternative TOP syntax (MS SQL Server compatible):**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

This is equivalent to `LIMIT m` and can be used for compatibility with Microsoft SQL Server queries.

**Select with offset:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

Skips the first `n` rows, then returns the next `m` rows.

In both forms, `n` and `m` must be non-negative integers.

## Negative limits {#negative-limits}

Select rows from the *end* of the result set using negative values:

| Syntax | Result |
|--------|--------|
| `LIMIT -m` | Last `m` rows |
| `LIMIT -m OFFSET -n` | Last `m` rows after skipping the last `n` rows |
| `LIMIT m OFFSET -n` | First `m` rows after skipping the last `n` rows |
| `LIMIT -m OFFSET n` | Last `m` rows after skipping the first `n` rows |

The `LIMIT -n, -m` syntax is equivalent to `LIMIT -m OFFSET -n`.

## Fractional limits {#fractional-limits}

Use decimal values between 0 and 1 to select a percentage of rows:

| Syntax | Result |
|--------|--------|
| `LIMIT 0.1` | First 10% of rows |
| `LIMIT 1 OFFSET 0.5` | The median row |
| `LIMIT 0.25 OFFSET 0.5` | Third quartile (25% of rows after skipping the first 50%) |

<Note>
- Fractions must be [Float64](/reference/data-types/float) values greater than 0 and less than 1.
- Fractional row counts are rounded to the next whole number.
</Note>

## Combining limit types {#combining-limit-types}

You can mix standard integers with fractional or negative offsets:

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

## LIMIT ... WITH TIES {#limit--with-ties-modifier}

The `WITH TIES` modifier includes additional rows that have the same `ORDER BY` values as the last row in your limit.

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

With `WITH TIES`, all rows matching the last value are included:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Row 6 is included because it has the same value (`2`) as row 5.

The same applies when the offset is specified with the `OFFSET` keyword:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Skipping the first 2 rows and taking 3 would normally return `1, 1, 2`, but the second `2` is included because it ties with the last row.

`WITH TIES` also works with negative limits and offsets. It includes additional rows that have the same `ORDER BY` values as the first selected row:

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Without `WITH TIES`, the result would be `1, 1, 2, 2`. With `WITH TIES`, three extra rows with value `1` are included because they tie with the first selected row.

This modifier can be combined with the [`ORDER BY ... WITH FILL`](/reference/statements/select/order-by#order-by-expr-with-fill-modifier) modifier.

## Considerations {#considerations}

**Non-deterministic results:** Without an [`ORDER BY`](/reference/statements/select/order-by) clause, the rows returned may be arbitrary and vary between query executions.

**Server-side limit:** The number of rows returned can also be affected by the [limit](/reference/settings/session-settings/other#limit) setting.

## See also {#see-also}

- [LIMIT BY](/reference/statements/select/limit-by) — Limits rows per group of values, useful for getting top N results within each category.
)DOCS_MD",
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
        .description = R"DOCS_MD(
A query with the `LIMIT n BY expressions` clause selects the first `n` rows for each distinct value of `expressions`. The key for `LIMIT BY` can contain any number of [expressions](/reference/syntax#expressions).

ClickHouse supports the following syntax variants:

- `LIMIT [offset_value, ]n BY expressions`
- `LIMIT n OFFSET offset_value BY expressions`

During query processing, ClickHouse selects data ordered by sorting key. The sorting key is set explicitly using an [ORDER BY](/reference/statements/select/order-by) clause or implicitly as a property of the table engine (row order is only guaranteed when using [ORDER BY](/reference/statements/select/order-by), otherwise the row blocks will not be ordered due to multi-threading). Then ClickHouse applies `LIMIT n BY expressions` and returns the first `n` rows for each distinct combination of `expressions`. If `OFFSET` is specified, then for each data block that belongs to a distinct combination of `expressions`, ClickHouse skips `offset_value` number of rows from the beginning of the block and returns a maximum of `n` rows as a result. If `offset_value` is bigger than the number of rows in the data block, ClickHouse returns zero rows from the block.

<Note>
`LIMIT BY` is not related to [LIMIT](/reference/statements/select/limit). They can both be used in the same query.
</Note>

If you want to use column numbers instead of column names in the `LIMIT BY` clause, enable the setting [enable_positional_arguments](/reference/settings/session-settings/enable-positional-arguments#enable_positional_arguments).

## Examples {#examples}

Sample table:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Queries:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

The `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query returns the same result.

The following query returns the top 5 referrers for each `domain, device_type` pair with a maximum of 100 rows in total (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

`LIMIT BY` also works with negative limits and offsets. Similar to the [negative LIMIT clause](/reference/statements/select/limit#negative-limits), you can use negative values with `LIMIT BY` to select rows from the *end* of each group.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

Returns the last 2 rows for each `id`. For `id = 1` we get rows `11` and `12`; for `id = 2` both rows are returned because the group has only 2 rows.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

Returns the second-to-last row of each `id`: the trailing `OFFSET -1` drops the last row per group, and the leading `-1` then keeps the last row of what remains.

Different sign `LIMIT` and `OFFSET` can be mixed as well. For example, to drop each group's first row and then keep the last 2 of what remains:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

For `id = 1`, the first row (`10`) is skipped; the last 2 of `11, 12` are both returned. For `id = 2`, the first row (`20`) is skipped, leaving only `21`.

## LIMIT BY ALL {#limit-by-all}

`LIMIT BY ALL` is equivalent to listing all the SELECT-ed expressions that are not aggregate functions.

For example:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

is the same as

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

For a special case that if there is a function having both aggregate functions and other fields as its arguments, the `LIMIT BY` keys will contain the maximum non-aggregate fields we can extract from it.

For example:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

is the same as

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

## Examples {#examples-limit-by-all}

Sample table:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Queries:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

The `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query returns the same result.

Using `LIMIT BY ALL`:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

This is equivalent to:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```
)DOCS_MD",
        .syntax = R"(
SELECT ... LIMIT [offset_value, ]n BY expressions ...
SELECT ... LIMIT n OFFSET offset_value BY expressions ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "LIMIT", "DISTINCT", "ORDER BY"},
    });

    factory.registerStatement("OFFSET FETCH",
    {
        .description = R"DOCS_MD(
`OFFSET` and `FETCH` allow you to retrieve data by portions. They specify a row block which you want to get by a single query.

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

The `offset_row_count` or `fetch_row_count` value can be a number or a literal constant. You can omit `fetch_row_count`; by default, it equals to 1.

`OFFSET` specifies the number of rows to skip before starting to return rows from the query result set. `OFFSET n` skips the first `n` rows from the result.

Negative OFFSET is supported: `OFFSET -n` skips the last `n` rows from the result.

Fractional OFFSET is also supported: `OFFSET n` - if 0 < n < 1, then the first n * 100% of the result is skipped.

Example:
    • `OFFSET 0.1` - skips the first 10% of the result.

> **Note**
> • The fraction must be a [Float64](/reference/data-types/float) number less than 1 and greater than zero.
> • If a fractional number of rows results from the calculation, it is rounded up to the next whole number.

The `FETCH` specifies the maximum number of rows that can be in the result of a query.

The `ONLY` option is used to return rows that immediately follow the rows omitted by the `OFFSET`. In this case the `FETCH` is an alternative to the [LIMIT](/reference/statements/select/limit) clause. For example, the following query

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

is identical to the query

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

The `WITH TIES` option is used to return any additional rows that tie for the last place in the result set according to the `ORDER BY` clause. For example, if `fetch_row_count` is set to 5 but two additional rows match the values of the `ORDER BY` columns in the fifth row, the result set will contain seven rows.

<Note>
According to the standard, the `OFFSET` clause must come before the `FETCH` clause if both are present.
</Note>

<Note>
The real offset can also depend on the [offset](/reference/settings/session-settings/other#offset) setting.
</Note>

## Examples {#examples}

Input table:

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

Usage of the `ONLY` option:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Usage of the `WITH TIES` option:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```
)DOCS_MD",
        .syntax = R"(
SELECT ... [OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]
SELECT ... [LIMIT [n, ]m] [OFFSET offset_row_count]
)",
        .parent = "SELECT",
        .related = {"SELECT", "LIMIT", "ORDER BY"},
    });
}

}
