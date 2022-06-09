#include <memory>
#include <queue>
//#include <sstream>
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

    String firstPart;
    for (std::size_t i = 0; i < temp.size() - 1; i++)
    {
        firstPart += temp[i];
    }
    if (temp.size() > 0)
    {
        return std::make_pair(firstPart, temp[temp.size() - 1]);
    }

    return std::make_pair("", "");
}

String ParserKQLSummarize::getBinGroupbyString(String exprBin)
{
    String column_name;
    bool bracket_start = false;
    bool comma_start = false;
    String bin_duration;

    for (std::size_t i = 0; i < exprBin.size(); i++)
    {
        if (comma_start && exprBin[i] != ')')
            bin_duration += exprBin[i];
        if (exprBin[i] == ',')
        {
            comma_start = true;
            bracket_start = false;
        }
        if (bracket_start == true)
            column_name += exprBin[i];
        if (exprBin[i] == '(')
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
    if (op_pos.size() != 1) // now only support one summarize
        return false;

    //summarize avg(age) by FirstName  ==> select FirstName,avg(Age) from Customers3 group by FirstName

    //summarize has syntax :

    //T | summarize [SummarizeParameters] [[Column =] Aggregation [, ...]] [by [Column =] GroupExpression [, ...]]

    //right now , we only support:

    //T | summarize Aggregation [, ...] [by GroupExpression  [, ...]]
    //Aggregation -> the Aggregation function on column
    //GroupExpression - > columns

    auto begin = pos;

    pos = op_pos.back();
    String exprAggregation;
    String exprGroupby;
    String exprColumns;
    String exprBin;
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
                if (String(pos->begin, pos->end) == "bin" || bin_function == true)
                {
                    bin_function = true;
                    exprBin += String(pos->begin, pos->end);
                    if (String(pos->begin, pos->end) == ")")
                    {
                        exprBin = getBinGroupbyString(exprBin);
                        exprGroupby += exprBin;
                        bin_function = false;
                    }
                }

                else
                    exprGroupby = exprGroupby + String(pos->begin, pos->end) + " ";
            }

            else
            {
                if (String(pos->begin, pos->end) == "=")
                {
                    std::pair<String, String> temp = removeLastWord(exprAggregation);
                    exprAggregation = temp.first;
                    column_name = temp.second;
                }
                else
                {
                    if (!column_name.empty())
                    {
                        exprAggregation = exprAggregation + String(pos->begin, pos->end);
                        character_passed++;
                        if (String(pos->begin, pos->end) == ")")
                        {
                            exprAggregation = exprAggregation + " AS " + column_name;
                            column_name = "";
                        }
                    }
                    else if (!bin_function)
                    {
                        exprAggregation = exprAggregation + String(pos->begin, pos->end) + " ";
                    }
                }
            }
        }
        ++pos;
    }

    if (exprGroupby.empty())
        exprColumns = exprAggregation;
    else
    {
        if (exprAggregation.empty())
            exprColumns = exprGroupby;
        else
            exprColumns = exprGroupby + "," + exprAggregation;
    }
    Tokens tokenColumns(exprColumns.c_str(), exprColumns.c_str() + exprColumns.size());
    IParser::Pos posColumns(tokenColumns, pos.max_depth);
    if (!ParserNotEmptyExpressionList(true).parse(posColumns, node, expected))
        return false;

    if (groupby)
    {
        Tokens tokenGroupby(exprGroupby.c_str(), exprGroupby.c_str() + exprGroupby.size());
        IParser::Pos postokenGroupby(tokenGroupby, pos.max_depth);
        if (!ParserNotEmptyExpressionList(false).parse(postokenGroupby, group_expression_list, expected))
            return false;
    }

    pos = begin;
    return true;
}

}
