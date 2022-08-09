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

    if (step_pos == begin)
        return false;

    if (String(from_pos->begin, from_pos->end) == "from")
    {
        ++from_pos;
        auto end_from_pos = (to_pos != begin) ? to_pos : step_pos;
        --end_from_pos;
        from_to_step.from = String(from_pos->begin, end_from_pos->end);
    }

    if (to_pos != begin)
    {   ++to_pos;
        --step_pos;
        from_to_step.to = String(to_pos->begin, step_pos->end);
        ++step_pos;
        ++step_pos;
    }
    --end_pos;
    from_to_step.step = String(step_pos->begin, end_pos->end);
    return true;
}


bool ParserKQLMakeSeries :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;

    auto begin = pos;

    pos = op_pos.back();

    String axis_column;
    String group_expression;

    ParserKeyword s_on("on");
    ParserKeyword s_by("by");

    ParserToken equals(TokenType::Equals);
    ParserToken comma(TokenType::Comma);

    AggregationColumns aggregation_columns;
    FromToStepClause from_to_step;

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

 // 'on' statement parameter, expecting scalar value of type 'int', 'long', 'real', 'datetime' or 'timespan'.

    if (s_by.ignore(pos, expected))
    {
        group_expression = getExprFromToken(pos);
        if (group_expression.empty())
            return false;
    }

    String subquery_columns;

    for (auto agg_column : aggregation_columns)
    {
        String column_str = std::format("{}({}) AS {}_ali", agg_column.aggregation_fun, agg_column.column, agg_column.alias);
        if (subquery_columns.empty())
            subquery_columns = column_str;
        else
            subquery_columns += ", "+ column_str;
    }

    ASTPtr sub_qurery_table;
    double step;
    String sub_query ;
    String main_query ;
    String group_by;

    String start_str = getExprFromToken(from_to_step.from, pos.max_depth);
    String end_str = getExprFromToken(from_to_step.to, pos.max_depth);
    String step_str = from_to_step.step;

    if (time_span.parseConstKQLTimespan(step_str))
    {
        step = time_span.toSeconds();

        auto bin_str =  std::format(" toUInt64(toFloat64(toDateTime64({},6,'UTC')) / {}) * {} AS {}_ali ", axis_column, step,step, axis_column);
        auto sub_sub_query = std::format(" (Select {},{}, {} FROM {} GROUP BY {},{}_ali ORDER BY {}_ali) ", group_expression, subquery_columns, bin_str, table_name, group_expression, axis_column, axis_column);

        auto start = std::format("toUInt64(toDateTime64({},6,'UTC'))", start_str);
        auto end = std::format("toUInt64(toDateTime64({},6,'UTC'))", end_str);
        auto range = std::format("range({},{}, toUInt64({}))", start, end, step);
        auto range_len = std::format("length({})", range);
        main_query = std::format("{} ", group_expression);

        auto axis_and_agg_alias_list = axis_column;
        auto final_axis_agg_alias_list =std::format("tupleElement(zipped,1) AS {}",axis_column); 
        int idx = 2;
        for (auto agg_column : aggregation_columns)
        {
            String agg_group_column = std::format("arrayConcat(groupArrayIf ({}_ali,{}_ali >= {} and {}_ali <= {}) as ga, arrayMap(x -> ({}),range(0,toUInt32 ({} - length(ga) < 0 ? 0 : {} - length(ga)),1) )) as {}",
            agg_column.alias, axis_column, start, axis_column, end, agg_column.default_value, range_len, range_len, agg_column.alias);
            main_query +=", " + agg_group_column;

            axis_and_agg_alias_list +=", " + agg_column.alias;
            final_axis_agg_alias_list += std::format(", tupleElement(zipped,{}) AS {}", idx, agg_column.alias);
        }
        auto axis_str = std::format("arrayDistinct(arrayConcat(groupArrayIf(toDateTime64({}_ali,6,'UTC'),{}_ali >= {} and {}_ali <= {}), arrayMap( x->(toDateTime64(x,6,'UTC')), {}) )) as {}",
            axis_column, axis_column, start, axis_column, end, range, axis_column);

        main_query += ", " + axis_str;
        auto sub_group_by = std::format("{}", group_expression);

        sub_query = std::format("( SELECT min({}_ali) AS low,max({}_ali) AS high, arraySort(arrayZip({})) as zipped, {} FROM {} GROUP BY {} )",
                                axis_column, axis_column,axis_and_agg_alias_list,main_query,sub_sub_query, sub_group_by);

        main_query = std::format("{},{}", group_expression, final_axis_agg_alias_list);

     }
    else
    {
        step = stod(step_str);

        sub_query = std::format("kql( {} | summarize {}, {} = toint({} / {}) * {} by {},{} )", 
                                        table_name, subquery_columns, axis_column, axis_column, step, subquery_columns, axis_column);
    }

    Tokens token_subquery(sub_query.c_str(), sub_query.c_str()+sub_query.size());
    IParser::Pos pos_subquery(token_subquery, pos.max_depth);

    if (!ParserTablesInSelectQuery().parse(pos_subquery, sub_qurery_table, expected))
        return false;
    tables = std::move(sub_qurery_table);

    String converted_columns =  main_query;

    Tokens token_converted_columns(converted_columns.c_str(), converted_columns.c_str() + converted_columns.size());
    IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, node, expected))
        return false;

    if (!group_by.empty())
    {
        String converted_groupby = group_by;

        Tokens token_converted_groupby(converted_groupby.c_str(), converted_groupby.c_str() + converted_groupby.size());
        IParser::Pos postoken_converted_groupby(token_converted_groupby, pos.max_depth);

        if (!ParserNotEmptyExpressionList(false).parse(postoken_converted_groupby, group_expression_list, expected))
            return false;
    }

    pos = begin;
    return true;

}

}
