#include <memory>
#include <queue>
#include <sstream>
#include <vector>
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
std::pair<String, String> removeLastWord(String input)
{
    std::istringstream ss(input);
    std::string token;
    std::vector<String> temp;

    while (std::getline(ss, token, ' '))
    {
        temp.push_back(token);
    }

    String firstPart;
    for (std::size_t i = 0; i < temp.size() - 1; i++)
    {
        firstPart += temp[i];
    }

    return std::make_pair(firstPart, temp[temp.size() - 1]);
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
                if (String(pos->begin, pos->end) == "bin")
                {
                    exprGroupby = exprGroupby + "round" + " ";
                    bin_function = true;
                }
                else
                    exprGroupby = exprGroupby + String(pos->begin, pos->end) + " ";
                    
                if (bin_function && last_string == "(")
                {
                    bin_column = String(pos->begin, pos->end);
                    bin_function = false;
                }

                last_string = String(pos->begin, pos->end);
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
                        if (String(pos->begin, pos->end) == ")") // was 4
                        {
                            exprAggregation = exprAggregation + " AS " + column_name;
                            column_name = "";
                        }
                    }
                    else
                    {
                        exprAggregation = exprAggregation + String(pos->begin, pos->end) + " ";
                    }
                }
            }
        }
        ++pos;
    }

    if(!bin_column.empty())
        exprGroupby = exprGroupby + " AS " + bin_column;

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
