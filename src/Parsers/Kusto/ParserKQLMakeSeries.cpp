#include <format>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

namespace DB
{

bool ParserKQLMakeSeries ::parseAggregationColumns(AggregationColumns & aggregation_columns, Pos & pos)
{
    std::unordered_set<String> allowed_aggregation(
        {"avg",
         "avgif",
         "count",
         "countif",
         "dcount",
         "dcountif",
         "max",
         "maxif",
         "min",
         "minif",
         "percentile",
         "take_any",
         "stdev",
         "sum",
         "sumif",
         "variance"});

    Expected expected;
    ParserKeyword s_default(Keyword::DEFAULT);
    ParserToken equals(TokenType::Equals);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserToken comma(TokenType::Comma);

    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String alias;
        String aggregation_fun;
        String column;
        double default_value = 0;

        String first_token(pos->begin, pos->end);

        ++pos;
        if (equals.ignore(pos, expected))
        {
            alias = std::move(first_token);
            aggregation_fun = String(pos->begin, pos->end);
            ++pos;
        }
        else
            aggregation_fun = std::move(first_token);

        if (allowed_aggregation.find(aggregation_fun) == allowed_aggregation.end())
            return false;

        if (open_bracket.ignore(pos, expected))
            column = String(pos->begin, pos->end);
        else
            return false;

        ++pos;
        if (!close_bracket.ignore(pos, expected))
            return false;

        if (s_default.ignore(pos, expected))
        {
            if (!equals.ignore(pos, expected))
                return false;

            default_value = std::stod(String(pos->begin, pos->end));
            ++pos;
        }
        if (alias.empty())
            alias = std::format("{}_{}", aggregation_fun, column);
        aggregation_columns.push_back(AggregationColumn(alias, aggregation_fun, column, default_value));

        if (!comma.ignore(pos, expected))
            break;
    }
    return true;
}

bool ParserKQLMakeSeries ::parseFromToStepClause(FromToStepClause & from_to_step, Pos & pos)
{
    auto begin = pos;
    auto from_pos = begin;
    auto to_pos = begin;
    auto step_pos = begin;
    auto end_pos = begin;

    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "from")
            from_pos = pos;
        if (String(pos->begin, pos->end) == "to")
            to_pos = pos;
        if (String(pos->begin, pos->end) == "step")
            step_pos = pos;
        if (String(pos->begin, pos->end) == "by")
        {
            end_pos = pos;
            break;
        }
        ++pos;
    }

    if (end_pos == begin)
        end_pos = pos;

    if (String(step_pos->begin, step_pos->end) != "step")
        return false;

    if (String(from_pos->begin, from_pos->end) == "from")
    {
        ++from_pos;
        auto end_from_pos = (to_pos != begin) ? to_pos : step_pos;
        --end_from_pos;
        from_to_step.from_str = String(from_pos->begin, end_from_pos->end);
    }

    if (String(to_pos->begin, to_pos->end) == "to")
    {
        ++to_pos;
        --step_pos;
        from_to_step.to_str = String(to_pos->begin, step_pos->end);
        ++step_pos;
    }
    --end_pos;
    ++step_pos;
    from_to_step.step_str = String(step_pos->begin, end_pos->end);

    if (String(step_pos->begin, step_pos->end) == "time" || String(step_pos->begin, step_pos->end) == "timespan"
        || ParserKQLDateTypeTimespan().parseConstKQLTimespan(from_to_step.step_str))
    {
        from_to_step.is_timespan = true;
        from_to_step.step = std::stod(getExprFromToken(from_to_step.step_str, pos.max_depth, pos.max_backtracks));
    }
    else
        from_to_step.step = std::stod(from_to_step.step_str);

    return true;
}

bool ParserKQLMakeSeries ::makeSeries(KQLMakeSeries & kql_make_series, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks)
{
    const uint64_t era_diff
        = 62135596800; // this magic number is the differicen is second form 0001-01-01 (Azure start time ) and 1970-01-01 (CH start time)

    String start_str, end_str;
    String sub_query, main_query;

    auto & aggregation_columns = kql_make_series.aggregation_columns;
    auto & from_to_step = kql_make_series.from_to_step;
    auto & subquery_columns = kql_make_series.subquery_columns;
    auto & axis_column = kql_make_series.axis_column;
    auto & group_expression = kql_make_series.group_expression;
    auto step = from_to_step.step;

    if (!kql_make_series.from_to_step.from_str.empty())
        start_str = getExprFromToken(kql_make_series.from_to_step.from_str, max_depth, max_backtracks);

    if (!kql_make_series.from_to_step.to_str.empty())
        end_str = getExprFromToken(from_to_step.to_str, max_depth, max_backtracks);

    auto date_type_cast = [&](String & src)
    {
        Tokens tokens(src.data(), src.data() + src.size(), 0, true);
        IParser::Pos pos(tokens, max_depth, max_backtracks);
        String res;
        while (isValidKQLPos(pos))
        {
            String tmp = String(pos->begin, pos->end);
            if (tmp == "parseDateTime64BestEffortOrNull")
                tmp = "toDateTime64";

            res = res.empty() ? tmp : res + " " + tmp;
            ++pos;
        }
        return res;
    };

    start_str = date_type_cast(start_str);
    end_str = date_type_cast(end_str);

    String bin_str, start, end;

    uint64_t diff = 0;
    String axis_column_format;
    String axis_str;

    auto get_group_expression_alias = [&]
    {
        std::vector<String> group_expression_tokens;
        Tokens tokens(group_expression.data(), group_expression.data() + group_expression.size(), 0, true);
        IParser::Pos pos(tokens, max_depth, max_backtracks);
        while (isValidKQLPos(pos))
        {
            if (String(pos->begin, pos->end) == "AS")
            {
                if (!group_expression_tokens.empty())
                    group_expression_tokens.pop_back();
                ++pos;
                group_expression_tokens.push_back(String(pos->begin, pos->end));
            }
            else
                group_expression_tokens.push_back(String(pos->begin, pos->end));
            ++pos;
        }
        String res;
        for (auto const & token : group_expression_tokens)
            res = res + token + " ";
        return res;
    };

    auto group_expression_alias = get_group_expression_alias();

    if (from_to_step.is_timespan)
    {
        axis_column_format = std::format("toFloat64(toDateTime64({}, 9, 'UTC'))", axis_column);
    }
    else
        axis_column_format = std::format("toFloat64({})", axis_column);

    if (!start_str.empty()) // has from
    {
        bin_str = std::format(
            "toFloat64({0}) + (toInt64((({1} - toFloat64({0})) / {2})) * {2}) AS {3}_ali",
            start_str,
            axis_column_format,
            step,
            axis_column);
        start = std::format("toUInt64({})", start_str);
    }
    else
    {
        if (from_to_step.is_timespan)
            diff = era_diff;
        bin_str = std::format(" toFloat64(toInt64(({0} + {1}) / {2}) * {2}) AS {3}_ali ", axis_column_format, diff, step, axis_column);
    }

    if (!end_str.empty())
        end = std::format("toUInt64({})", end_str);

    String range, condition;

    if (!start_str.empty() && !end_str.empty())
    {
        range = std::format("range({}, {}, toUInt64({}))", start, end, step);
        condition = std::format("where toInt64({0}) >= {1} and toInt64({0}) < {2}", axis_column_format, start, end);
    }
    else if (start_str.empty() && !end_str.empty())
    {
        range = std::format("range(low, {} + {}, toUInt64({}))", end, diff, step);
        condition = std::format("where toInt64({0}) - {1} < {2}", axis_column_format, diff, end);
    }
    else if (!start_str.empty() && end_str.empty())
    {
        range = std::format("range({}, high, toUInt64({}))", start, step);
        condition = std::format("where toInt64({}) >= {}", axis_column_format, start);
    }
    else
    {
        range = std::format("range(low, high, toUInt64({}))", step);
        condition = " ";
    }

    auto range_len = std::format("length({})", range);

    String sub_sub_query;
    if (group_expression.empty())
        sub_sub_query = std::format(
            " (Select {0}, {1} FROM {2} {4} GROUP BY  {3}_ali ORDER BY {3}_ali) ",
            subquery_columns,
            bin_str,
            "table_name",
            axis_column,
            condition);
    else
        sub_sub_query = std::format(
            " (Select {0}, {1}, {2} FROM {3} {5} GROUP BY {0}, {4}_ali ORDER BY {4}_ali) ",
            group_expression,
            subquery_columns,
            bin_str,
            "table_name",
            axis_column,
            condition);

    ASTPtr sub_query_node;

    if (!ParserSimpleCHSubquery(select_node).parseByString(sub_sub_query, sub_query_node, max_depth, max_backtracks))
        return false;
    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(sub_query_node));

    if (!group_expression.empty())
        main_query = std::format("{} ", group_expression_alias);

    auto axis_and_agg_alias_list = axis_column;
    auto final_axis_agg_alias_list = std::format("tupleElement(zipped,1) AS {}", axis_column);
    int idx = 2;
    for (auto agg_column : aggregation_columns)
    {
        String agg_group_column = std::format(
            "arrayConcat(groupArray({}_ali) as ga, arrayMap(x -> ({}),range(0, toUInt32({} - length(ga) < 0 ? 0 : {} - length(ga)),1)))"
            "as {}",
            agg_column.alias,
            agg_column.default_value,
            range_len,
            range_len,
            agg_column.alias);
        main_query = main_query.empty() ? agg_group_column : main_query + ", " + agg_group_column;

        axis_and_agg_alias_list += ", " + agg_column.alias;
        final_axis_agg_alias_list += std::format(", tupleElement(zipped,{}) AS {}", idx, agg_column.alias);
    }

    if (from_to_step.is_timespan)
        axis_str = std::format(
            "arrayDistinct(arrayConcat(groupArray(toDateTime64({0}_ali - {1},9,'UTC')), arrayMap(x->(toDateTime64(x - {1} ,9,'UTC')),"
            "{2}))) as {0}",
            axis_column,
            diff,
            range);
    else
        axis_str
            = std::format("arrayDistinct(arrayConcat(groupArray({0}_ali), arrayMap(x->(toFloat64(x)), {1}))) as {0}", axis_column, range);

    main_query += ", " + axis_str;
    auto sub_group_by = group_expression.empty() ? "" : std::format("GROUP BY {}", group_expression_alias);

    sub_query = std::format(
        "( SELECT toUInt64(min({}_ali)) AS low, toUInt64(max({}_ali))+ {} AS high, arraySort(arrayZip({})) as zipped, {} FROM {} {} )",
        axis_column,
        axis_column,
        step,
        axis_and_agg_alias_list,
        main_query,
        sub_sub_query,
        sub_group_by);

    if (group_expression.empty())
        main_query = std::format("{}", final_axis_agg_alias_list);
    else
        main_query = std::format("{},{}", group_expression_alias, final_axis_agg_alias_list);

    if (!ParserSimpleCHSubquery(select_node).parseByString(sub_query, sub_query_node, max_depth, max_backtracks))
        return false;
    select_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(sub_query_node));

    kql_make_series.sub_query = std::move(sub_query);
    kql_make_series.main_query = std::move(main_query);

    return true;
}

bool ParserKQLMakeSeries ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_by(Keyword::BY);

    ParserToken equals(TokenType::Equals);
    ParserToken comma(TokenType::Comma);

    ASTPtr select_expression_list;

    KQLMakeSeries kql_make_series;
    auto & aggregation_columns = kql_make_series.aggregation_columns;
    auto & from_to_step = kql_make_series.from_to_step;
    auto & subquery_columns = kql_make_series.subquery_columns;
    auto & axis_column = kql_make_series.axis_column;
    auto & group_expression = kql_make_series.group_expression;

    ParserKQLDateTypeTimespan time_span;

    //const auto make_series_parameters = getMakeSeriesParameters(pos);

    if (!parseAggregationColumns(aggregation_columns, pos))
        return false;

    if (!s_on.ignore(pos, expected))
        return false;

    axis_column = String(pos->begin, pos->end);
    ++pos;

    if (!parseFromToStepClause(from_to_step, pos))
        return false;

    if (s_by.ignore(pos, expected))
    {
        group_expression = getExprFromToken(pos);
        if (group_expression.empty())
            return false;
    }

    for (auto agg_column : aggregation_columns)
    {
        String column_str = std::format("{}({}) AS {}_ali", agg_column.aggregation_fun, agg_column.column, agg_column.alias);
        if (subquery_columns.empty())
            subquery_columns = column_str;
        else
            subquery_columns += ", " + column_str;
    }

    makeSeries(kql_make_series, node, pos.max_depth, pos.max_backtracks);

    Tokens token_main_query(kql_make_series.main_query.data(), kql_make_series.main_query.data() + kql_make_series.main_query.size(), 0, true);
    IParser::Pos pos_main_query(token_main_query, pos.max_depth, pos.max_backtracks);

    if (!ParserNotEmptyExpressionList(true).parse(pos_main_query, select_expression_list, expected))
        return false;
    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    pos = begin;
    return true;
}

}
