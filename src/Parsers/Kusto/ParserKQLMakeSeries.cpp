#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <format>

namespace DB
{

bool ParserKQLMakeSeries :: parseAggregationColumns(AggregationColumns & aggregation_columns, Pos & pos)
{
 std::unordered_set<String> allowed_aggregation 
    ({
        "avg",
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
        "variance"
    });

    Expected expected;
    ParserKeyword s_default("default");
    ParserToken equals(TokenType::Equals);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserToken comma(TokenType::Comma);

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String alias;
        String aggregation_fun;
        String column;
        double default_value = 0;
        
        String first_token(pos->begin,pos->end);

        ++pos;
        if (equals.ignore(pos, expected))
        {
            alias = std::move(first_token);
            aggregation_fun = String(pos->begin,pos->end);
        }
        else
            aggregation_fun = std::move(first_token);

        if (allowed_aggregation.find(aggregation_fun) == allowed_aggregation.end())
            return false;

        ++pos;
        if (open_bracket.ignore(pos, expected))
            column = String(pos->begin,pos->end);
        else
            return false;

        ++pos;
        if (!close_bracket.ignore(pos, expected))
            return false;

        if (s_default.ignore(pos, expected))
        {
            if (!equals.ignore(pos, expected))
                return false;

            default_value = std::stod(String(pos->begin,pos->end));
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

bool ParserKQLMakeSeries :: parseFromToStepClause(FromToStepClause & from_to_step, Pos & pos)
{
    auto begin = pos;
    auto from_pos = begin;
    auto to_pos = begin;
    auto step_pos = begin;
    auto end_pos = begin;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if ( String(pos->begin, pos->end) == "from") 
            from_pos = pos;
        if ( String(pos->begin, pos->end) == "to") 
            to_pos = pos;
        if ( String(pos->begin, pos->end) == "step") 
            step_pos = pos;
        if ( String(pos->begin, pos->end) == "by") 
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
    {   ++to_pos;
        --step_pos;
        from_to_step.to_str = String(to_pos->begin, step_pos->end);
        ++step_pos;
    }
    --end_pos;
    ++step_pos;
    from_to_step.step_str = String(step_pos->begin, end_pos->end);

    if (String(step_pos->begin, step_pos->end) == "time" || String(step_pos->begin, step_pos->end) == "timespan" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(from_to_step.step_str))
    {
        from_to_step.is_timespan = true;
        from_to_step.step = std::stod(getExprFromToken(from_to_step.step_str, pos.max_depth));
    }
    else
        from_to_step.step = std::stod(from_to_step.step_str);

    return true;
}


void ParserKQLMakeSeries :: makeNumericSeries(KQLMakeSeries & kql_make_series, const uint32_t & max_depth)
{
    String start_str, end_str;
    String sub_query, main_query;

    auto & aggregation_columns = kql_make_series.aggregation_columns;
    auto & from_to_step  = kql_make_series.from_to_step;
    auto & subquery_columns = kql_make_series.subquery_columns;
    auto & axis_column = kql_make_series.axis_column;
    auto & group_expression = kql_make_series.group_expression;
    auto step = from_to_step.step;

    if (!kql_make_series.from_to_step.from_str.empty())
        start_str = getExprFromToken(kql_make_series.from_to_step.from_str, max_depth);

    if (!kql_make_series.from_to_step.to_str.empty())
        end_str = getExprFromToken(from_to_step.to_str, max_depth);

    String bin_str, start, end;

    if (!start_str.empty()) // has from
    {
        bin_str =  std::format(" toFloat64({0}) + (toInt64(((toFloat64({1}) - toFloat64({0})) / {2}) ) * {2}) AS {1}_ali ",
                                start_str, axis_column, step);
        start = std::format("toUInt64({})", start_str);
    }
    else
    {
        bin_str =  std::format(" toFloat64(toInt64((toFloat64({0}) ) / {1}) * {1}) AS {0}_ali ",
                                axis_column, step);
    }

    auto sub_sub_query = std::format(" (Select {0}, {1}, {2} FROM {3} GROUP BY {0}, {4}_ali ORDER BY {4}_ali) ", group_expression, subquery_columns, bin_str, table_name, axis_column);

    if (!end_str.empty())
        end = std::format("toUInt64({})", end_str);

    String range, condition;
    if (!start_str.empty() && !end_str.empty())
    {
        range = std::format("range({},{}, toUInt64({}))", start, end, step);
        condition = std::format("{0}_ali >= {1} and {0}_ali <= {2}", axis_column, start, end);
    }
    else if (start_str.empty() && !end_str.empty())
    {
        range = std::format("range(low, {} , toUInt64({}))",  end,  step);
        condition = std::format("{}_ali <= {}", axis_column, end);
    }
    else if (!start_str.empty() && end_str.empty())
    {
        range = std::format("range({}, high, toUInt64({}))", start, step);
        condition = std::format("{}_ali >= {}", axis_column, start);
    }
    else
    {
        range = std::format("range(low, high, toUInt64({}))", step);
        condition = "1"; //true
    }

    auto range_len = std::format("length({})", range);
    main_query = std::format("{} ", group_expression);

    auto axis_and_agg_alias_list = axis_column;
    auto final_axis_agg_alias_list =std::format("tupleElement(zipped,1) AS {}",axis_column); //tupleElement(pp,2) as PriceAvg ,tupleElement(pp,1)
    int idx = 2;
    for (auto agg_column : aggregation_columns)
    {
        String agg_group_column = std::format("arrayConcat(groupArrayIf ({}_ali,{}) as ga, arrayMap(x -> ({}),range(0,toUInt32 ({} - length(ga) < 0 ? 0 : {} - length(ga)),1) )) as {}",
        agg_column.alias, condition, agg_column.default_value, range_len, range_len, agg_column.alias);
        main_query +=", " + agg_group_column;

        axis_and_agg_alias_list +=", " + agg_column.alias;
        final_axis_agg_alias_list += std::format(", tupleElement(zipped,{}) AS {}", idx, agg_column.alias);
    }
    
    auto axis_str = std::format("arrayDistinct(arrayConcat(groupArrayIf({0}_ali, {1}), arrayMap( x->(toFloat64(x)), {2})) ) as {0}",
        axis_column, condition,range);

    main_query += ", " + axis_str;
    auto sub_group_by = std::format("{}", group_expression);

    sub_query = std::format("( SELECT toUInt64(min({}_ali)) AS low, toUInt64(max({}_ali))+ {} AS high, arraySort(arrayZip({})) as zipped, {} FROM {} GROUP BY {} )",
                            axis_column, axis_column,step, axis_and_agg_alias_list,main_query,sub_sub_query, sub_group_by);

    main_query = std::format("{},{}", group_expression, final_axis_agg_alias_list);
    
    kql_make_series.sub_query = std::move(sub_query);
    kql_make_series.main_query = std::move(main_query);
}

void ParserKQLMakeSeries :: makeTimeSeries(KQLMakeSeries & kql_make_series, const uint32_t & max_depth)
{
    const uint64_t era_diff = 62135596800;  // this magic number is the differicen is second form 0001-01-01 (Azure start time ) and 1970-01-01 (CH start time)

    String start_str, end_str;
    String sub_query, main_query;

    auto & aggregation_columns = kql_make_series.aggregation_columns;
    auto & from_to_step  = kql_make_series.from_to_step;
    auto & subquery_columns = kql_make_series.subquery_columns;
    auto & axis_column = kql_make_series.axis_column;
    auto & group_expression = kql_make_series.group_expression;
    auto step = from_to_step.step;

    if (!kql_make_series.from_to_step.from_str.empty())
        start_str = getExprFromToken(kql_make_series.from_to_step.from_str, max_depth);

    if (!kql_make_series.from_to_step.to_str.empty())
        end_str = getExprFromToken(from_to_step.to_str, max_depth);

    String bin_str, start, end;

    uint64_t diff = 0;
    if (!start_str.empty()) // has from
    {
        bin_str =  std::format(" toFloat64(toDateTime64({0}, 9, 'UTC')) + (toInt64(((toFloat64(toDateTime64({1}, 9, 'UTC')) - toFloat64(toDateTime64({0}, 9, 'UTC'))) / {2}) ) * {2}) AS {1}_ali ",
                                start_str, axis_column, step);
        start = std::format("toUInt64(toDateTime64({},9,'UTC'))", start_str);
    }
    else
    {
        bin_str =  std::format(" toInt64((toFloat64(toDateTime64({0}, 9, 'UTC')) + {1}) / {2}) * {2} AS {0}_ali ",
                                axis_column, era_diff, step);
        diff = era_diff;
    }

    auto sub_sub_query = std::format(" (Select {0}, {1}, {2} FROM {3} GROUP BY {0}, {4}_ali ORDER BY {4}_ali) ", group_expression, subquery_columns, bin_str, table_name, axis_column);

    if (!end_str.empty())
        end = std::format("toUInt64(toDateTime64({}, 9, 'UTC'))", end_str);

    String range, condition;
    if (!start_str.empty() && !end_str.empty())
    {
        range = std::format("range({},{}, toUInt64({}))", start, end, step);
        condition = std::format("{0}_ali >= {1} and {0}_ali <= {2}", axis_column, start, end);
    }
    else if (start_str.empty() && !end_str.empty())
    {
        range = std::format("range(low, {} + {}, toUInt64({}))",  end, era_diff, step);
        condition = std::format("{0}_ali - {1} < {2}", axis_column, era_diff, end);
    }
    else if (!start_str.empty() && end_str.empty())
    {
        range = std::format("range({}, high, toUInt64({}))", start, step);
        condition = std::format("{}_ali >= {}", axis_column, start);
    }
    else
    {
        range = std::format("range(low, high, toUInt64({}))", step);
        condition = "1"; //true
    }

    auto range_len = std::format("length({})", range);
    main_query = std::format("{} ", group_expression);

    auto axis_and_agg_alias_list = axis_column;
    auto final_axis_agg_alias_list =std::format("tupleElement(zipped,1) AS {}",axis_column); //tupleElement(pp,2) as PriceAvg ,tupleElement(pp,1)
    int idx = 2;
    for (auto agg_column : aggregation_columns)
    {
        String agg_group_column = std::format("arrayConcat(groupArrayIf ({}_ali,{}) as ga, arrayMap(x -> ({}),range(0,toUInt32 ({} - length(ga) < 0 ? 0 : {} - length(ga)),1) )) as {}",
        agg_column.alias, condition, agg_column.default_value, range_len, range_len, agg_column.alias);
        main_query +=", " + agg_group_column;

        axis_and_agg_alias_list +=", " + agg_column.alias;
        final_axis_agg_alias_list += std::format(", tupleElement(zipped,{}) AS {}", idx, agg_column.alias);
    }
    auto axis_str = std::format("arrayDistinct(arrayConcat(groupArrayIf(toDateTime64({0}_ali - {1},9,'UTC'), {2}), arrayMap( x->(toDateTime64(x - {1} ,9,'UTC')), {3}) )) as {0}",
        axis_column, diff, condition,range);

    main_query += ", " + axis_str;
    auto sub_group_by = std::format("{}", group_expression);

    sub_query = std::format("( SELECT toUInt64(min({}_ali)) AS low, toUInt64(max({}_ali))+ {} AS high, arraySort(arrayZip({})) as zipped, {} FROM {} GROUP BY {} )",
                            axis_column, axis_column,step, axis_and_agg_alias_list, main_query, sub_sub_query,  sub_group_by);

    main_query = std::format("{},{}", group_expression, final_axis_agg_alias_list);

    kql_make_series.sub_query = std::move(sub_query);
    kql_make_series.main_query = std::move(main_query);
}

bool ParserKQLMakeSeries :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;

    auto begin = pos;
    pos = op_pos.back();

    ParserKeyword s_on("on");
    ParserKeyword s_by("by");

    ParserToken equals(TokenType::Equals);
    ParserToken comma(TokenType::Comma);

    ASTPtr sub_qurery_table;

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

    axis_column =  String(pos->begin, pos->end);
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
            subquery_columns += ", "+ column_str;
    }

    if (from_to_step.is_timespan)
        makeTimeSeries(kql_make_series, pos.max_depth);
    else
        makeNumericSeries(kql_make_series, pos.max_depth);

    Tokens token_subquery(kql_make_series.sub_query.c_str(), kql_make_series.sub_query.c_str() + kql_make_series.sub_query.size());
    IParser::Pos pos_subquery(token_subquery, pos.max_depth);

    if (!ParserTablesInSelectQuery().parse(pos_subquery, sub_qurery_table, expected))
        return false;
    tables = std::move(sub_qurery_table);

    Tokens token_main_query(kql_make_series.main_query.c_str(), kql_make_series.main_query.c_str() + kql_make_series.main_query.size());
    IParser::Pos pos_main_query(token_main_query, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_main_query, node, expected))
        return false;

    pos = begin;
    return true;

}
}
