#include <memory>
#include <queue>
#include <vector>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>
#include <format>

namespace DB
{

bool ParserKQLSummarize::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_expression_list;
    ASTPtr group_expression_list;

    String expr_aggregation;
    String expr_groupby;
    String expr_columns;
    bool groupby = false;
    auto column_begin_pos = pos;

    uint16_t bracket_count = 0;
    int32_t new_column_index = 1;

    std::vector<String> expr_aggregations;
    std::vector<String> expr_groupbys;

    std::unordered_set<String> aggregate_functions(
        {"arg_max",
         "arg_min",
         "avg",
         "avgif",
         "binary_all_and",
         "binary_all_or",
         "binary_all_xor",
         "buildschema",
         "count",
         "countif",
         "dcount",
         "dcountif",
         "make_bag",
         "make_bag_if",
         "make_list",
         "make_list_if",
         "make_list_with_nulls",
         "make_set",
         "make_set_if",
         "max",
         "maxif",
         "min",
         "minif",
         "percentile",
         "percentilew",
         "percentiles",
         "percentiles_array",
         "percentilesw",
         "percentilesw_array",
         "stdev",
         "stdevif",
         "sum",
         "sumif",
         "take_any",
         "take_anyif",
         "variance",
         "varianceif"});

    auto apply_aliais = [&](Pos & begin_pos, Pos & end_pos, bool is_groupby)
    {
        auto expr = String(begin_pos->begin, end_pos->end);
        auto equal_pos = begin_pos;
        ++equal_pos;
        if (!is_groupby)
        {
            if (String(equal_pos->begin, equal_pos->end) != "=")
            {
                String alias;
                String aggregate_fun = String(begin_pos->begin, begin_pos->end);
                if (aggregate_functions.find(aggregate_fun) == aggregate_functions.end())
                {
                    alias = std::format("Columns{}", new_column_index);
                    ++new_column_index;
                }
                else
                {
                    alias = std::format("{}_", aggregate_fun);
                    auto agg_colum_pos = begin_pos;
                    ++agg_colum_pos;
                    ++agg_colum_pos;
                    ++agg_colum_pos;
                    if (agg_colum_pos->type == TokenType::Comma || agg_colum_pos->type == TokenType::ClosingRoundBracket)
                    {
                        --agg_colum_pos;
                        if (agg_colum_pos->type != TokenType::ClosingRoundBracket)
                            alias = alias + String(agg_colum_pos->begin, agg_colum_pos->end);
                    }
                }
                expr = std::format("{} = {}", alias, expr);
            }
            expr_aggregations.push_back(expr);
        }
        else
        {
            if (String(equal_pos->begin, equal_pos->end) != "=")
            {
                String groupby_fun = String(begin_pos->begin, begin_pos->end);
                if (!equal_pos.isValid() || equal_pos->type == TokenType::Comma || equal_pos->type == TokenType::Semicolon
                    || equal_pos->type == TokenType::PipeMark)
                {
                    expr = groupby_fun;
                }
                else
                {
                    String alias;
                    if (groupby_fun == "bin" || groupby_fun == "bin_at")
                    {
                        auto bin_colum_pos = begin_pos;
                        ++bin_colum_pos;
                        ++bin_colum_pos;
                        alias = String(bin_colum_pos->begin, bin_colum_pos->end);
                        ++bin_colum_pos;
                        if (bin_colum_pos->type != TokenType::Comma)
                            alias.clear();
                    }
                    if (alias.empty())
                    {
                        alias = std::format("Columns{}", new_column_index);
                        ++new_column_index;
                    }

                    expr = std::format("{} = {}", alias, expr);
                }
            }
            expr_groupbys.push_back(expr);
        }
    };

    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;

        if (pos->type == TokenType::ClosingRoundBracket)
            --bracket_count;

        if ((bracket_count == 0 and pos->type == TokenType::Comma) || String(pos->begin, pos->end) == "by")
        {
            auto end_pos = pos;
            --end_pos;
            apply_aliais(column_begin_pos, end_pos, groupby);
            if (String(pos->begin, pos->end) == "by")
                groupby = true;
            column_begin_pos = pos;
            ++column_begin_pos;
        }
        ++pos;
    }
    --pos;
    apply_aliais(column_begin_pos, pos, groupby);

    for (auto const & expr : expr_aggregations)
        expr_aggregation = expr_aggregation.empty() ? expr : expr_aggregation + "," + expr;

    for (auto const & expr : expr_groupbys)
        expr_groupby = expr_groupby.empty() ? expr : expr_groupby + "," + expr;

    if (!expr_groupby.empty())
        expr_columns = expr_groupby;

    if (!expr_aggregation.empty())
    {
        if (expr_columns.empty())
            expr_columns = expr_aggregation;
        else
            expr_columns = expr_columns + "," + expr_aggregation;
    }

    String converted_columns = getExprFromToken(expr_columns, pos.max_depth, pos.max_backtracks);

    Tokens token_converted_columns(converted_columns.data(), converted_columns.data() + converted_columns.size(), 0, true);
    IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth, pos.max_backtracks);

    if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, select_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    if (groupby)
    {
        String converted_groupby = getExprFromToken(expr_groupby, pos.max_depth, pos.max_backtracks);

        Tokens token_converted_groupby(converted_groupby.data(), converted_groupby.data() + converted_groupby.size(), 0, true);
        IParser::Pos postoken_converted_groupby(token_converted_groupby, pos.max_depth, pos.max_backtracks);

        if (!ParserNotEmptyExpressionList(false).parse(postoken_converted_groupby, group_expression_list, expected))
            return false;
        node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    }

    return true;
}
}
