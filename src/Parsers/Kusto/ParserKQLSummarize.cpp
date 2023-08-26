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
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>

namespace DB
{
std::pair<String, String> ParserKQLSummarize::removeLastWord(String input)
{
    ReadBufferFromString in(input);
    String token;
    std::vector<String> temp;

    while (!in.eof())
    {
        readStringUntilWhitespace(token, in);
        if (in.eof())
            break;
        skipWhitespaceIfAny(in);
        temp.push_back(token);
    }

    String first_part;
    for (std::size_t i = 0; i < temp.size() - 1; i++)
    {
        first_part += temp[i];
    }
    if (!temp.empty())
    {
        return std::make_pair(first_part, temp[temp.size() - 1]);
    }
    if (!temp.empty())
    {
        return std::make_pair(first_part, temp[temp.size() - 1]);
    }

    return std::make_pair("", "");
}

String ParserKQLSummarize::getBinGroupbyString(String expr_bin)
{
    String column_name;
    bool bracket_start = false;
    bool comma_start = false;
    String bin_duration;

    for (char ch : expr_bin)
    {
        if (comma_start && ch != ')')
            bin_duration += ch;
        if (ch == ',')
        {
            comma_start = true;
            bracket_start = false;
        }
        if (bracket_start)
            column_name += ch;
        if (ch == '(')
            bracket_start = true;
    }

    std::size_t len = bin_duration.size();
    char bin_type = bin_duration[len - 1]; // y, d, h, m, s
    if ((bin_type != 'y') && (bin_type != 'd') && (bin_type != 'h') && (bin_type != 'm') && (bin_type != 's'))
    {
        return "toInt32(" + column_name + "/" + bin_duration + ") * " + bin_duration + " AS bin_int";
    }
    bin_duration = bin_duration.substr(0, len - 1);

    switch (bin_type)
    {
        case 'y':
            return "toDateTime(toInt32((toFloat32(toDateTime(" + column_name + ")) / (12*30*86400)) / " + bin_duration + ") * ("
                + bin_duration + " * (12*30*86400))) AS bin_year";
        case 'd':
            return "toDateTime(toInt32((toFloat32(toDateTime(" + column_name + ")) / 86400) / " + bin_duration + ") * (" + bin_duration
                + " * 86400)) AS bin_day";
        case 'h':
            return "toDateTime(toInt32((toFloat32(toDateTime(" + column_name + ")) / 3600) / " + bin_duration + ") * (" + bin_duration
                + " * 3600)) AS bin_hour";
        case 'm':
            return "toDateTime(toInt32((toFloat32(toDateTime(" + column_name + ")) / 60) / " + bin_duration + ") * (" + bin_duration
                + " * 60)) AS bin_minute";
        case 's':
            return "toDateTime(" + column_name + ") AS bin_sec";
        default:
            return "";
    }
}

bool ParserKQLSummarize ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;
    if (op_pos.size() > 2) // now only support max 2 summarize
        return false;

    auto begin = pos;
    ASTPtr sub_qurery_table;

// rewrite this part, make it resusable (may contains bin etc, and please inmplement summarize age= avg(Age) for sub query too):
    if (op_pos.size() == 2)
    {
        bool groupby = false;
        auto sub_pos = op_pos.front();
        String sub_aggregation;
        String sub_groupby;
        String sub_columns;
        while (!sub_pos->isEnd() && sub_pos->type != TokenType::PipeMark && sub_pos->type != TokenType::Semicolon)
        {
            if (String(sub_pos->begin,sub_pos->end) == "by")
                groupby = true;
            else 
            {
                if (groupby) 
                    sub_groupby = sub_groupby + String(sub_pos->begin,sub_pos->end) +" ";
                else
                    sub_aggregation = sub_aggregation + String(sub_pos->begin,sub_pos->end) +" ";
            }
            ++sub_pos;
        }

        String sub_query;
        if (sub_groupby.empty())
        {
            sub_columns =sub_aggregation;
            sub_query = "(SELECT " + sub_columns+ " FROM "+ table_name+")";
        }
        else
        {
            if (sub_aggregation.empty())
                sub_columns = sub_groupby;
            else
                sub_columns = sub_groupby + "," + sub_aggregation;
            sub_query = "SELECT " + sub_columns+ " FROM "+ table_name + " GROUP BY "+sub_groupby+"";
        }

        Tokens token_subquery(sub_query.c_str(), sub_query.c_str()+sub_query.size());
        IParser::Pos pos_subquery(token_subquery, pos.max_depth);

        String converted_columns =  getExprFromToken(pos_subquery);
        converted_columns = "(" + converted_columns + ")";
        
        //std::cout << "MALLIK converted_columns: " << converted_columns << std::endl;
        
        Tokens token_converted_columns(converted_columns.c_str(), converted_columns.c_str() + converted_columns.size());
        IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth);

        //if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, node, expected))
            //return false;
        if (!ParserTablesInSelectQuery().parse(pos_converted_columns, sub_qurery_table, expected))
            return false;
        tables = sub_qurery_table;
    }


    pos = op_pos.back();
    String expr_aggregation;
    String expr_groupby;
    String expr_columns;
    String expr_bin;
    bool groupby = false;
    bool bin_function = false;
    String bin_column;
    String last_string;
    String column_name;
    int character_passed = 0;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "by")
            groupby = true;
        else
        {
            if (groupby)
            {
                if (String(pos->begin, pos->end) == "bin" || bin_function)
                {
                    bin_function = true;
                    expr_bin += String(pos->begin, pos->end);
                    if (String(pos->begin, pos->end) == ")")
                    {
                        expr_bin = getBinGroupbyString(expr_bin);
                        expr_groupby += expr_bin;
                        bin_function = false;
                    }
                }

                else
                    expr_groupby = expr_groupby + String(pos->begin, pos->end) + " ";
            }

            else
            {
                /*if (String(pos->begin, pos->end) == "=")
                {
                    std::pair<String, String> temp = removeLastWord(expr_aggregation);
                    expr_aggregation = temp.first;
                    column_name = temp.second;
                }*/
                //else
                //{
                    if (!column_name.empty())
                    {
                        expr_aggregation = expr_aggregation + String(pos->begin, pos->end);
                        character_passed++;
                        if (String(pos->begin, pos->end) == ")")
                        {
                            expr_aggregation = expr_aggregation + " AS " + column_name;
                            column_name = "";
                        }
                    }
                    else if (!bin_function)
                    {
                        expr_aggregation = expr_aggregation + String(pos->begin, pos->end) + " ";
                    }
                //}
            }
        }
        ++pos;
    }

    if (expr_groupby.empty())
        expr_columns = expr_aggregation;
    else
    {
        if (expr_aggregation.empty())
            expr_columns = expr_groupby;
        else
            expr_columns = expr_groupby + "," + expr_aggregation;
    }
    
    
    /*
    Original

    Tokens token_columns(expr_columns.c_str(), expr_columns.c_str() + expr_columns.size());
    IParser::Pos pos_columns(token_columns, pos.max_depth);
    if (!ParserNotEmptyExpressionList(true).parse(pos_columns, node, expected))
        return false;

    if (groupby)
    {
        Tokens token_groupby(expr_groupby.c_str(), expr_groupby.c_str() + expr_groupby.size());
        IParser::Pos postoken_groupby(token_groupby, pos.max_depth);
        if (!ParserNotEmptyExpressionList(false).parse(postoken_groupby, group_expression_list, expected))
            return false;
    }
    */

   // For function
    Tokens token_columns(expr_columns.c_str(), expr_columns.c_str() + expr_columns.size());
    IParser::Pos pos_columns(token_columns, pos.max_depth);

    String converted_columns =  getExprFromToken(pos_columns);

    Tokens token_converted_columns(converted_columns.c_str(), converted_columns.c_str() + converted_columns.size());
    IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, node, expected))
        return false;

    if (groupby)
    {
        Tokens token_groupby(expr_groupby.c_str(), expr_groupby.c_str() + expr_groupby.size());
        IParser::Pos postoken_groupby(token_groupby, pos.max_depth);

        String converted_groupby =  getExprFromToken(postoken_groupby);

        Tokens token_converted_groupby(converted_groupby.c_str(), converted_groupby.c_str() + converted_groupby.size());
        IParser::Pos postoken_converted_groupby(token_converted_groupby, pos.max_depth);

        if (!ParserNotEmptyExpressionList(false).parse(postoken_converted_groupby, group_expression_list, expected))
            return false;
    }



    pos = begin;
    return true;
}

}
