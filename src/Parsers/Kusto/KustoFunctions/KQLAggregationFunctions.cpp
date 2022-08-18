#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

bool ArgMax::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"argMax");
}

bool ArgMin::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"argMin");
}

bool Avg::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"avg");
}

bool AvgIf::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"avgIf");
}

bool BinaryAllAnd::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"groupBitAnd");
}

bool BinaryAllOr::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"groupBitOr");
}

bool BinaryAllXor::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"groupBitXor");
}

bool BuildSchema::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Count::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"count");
}

bool CountIf::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"countIf");
}

bool DCount::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name,pos);
    
    out = "count ( DISTINCT " + value + " ) ";
    return true;
}

bool DCountIf::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name,pos);
    ++pos;
    String condition = getConvertedArgument(fn_name,pos);
    out = "countIf ( DISTINCT " + value + " , " + condition + " ) ";
    return true;
}

bool MakeBag::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeBagIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeList::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name,pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name,pos);
        out = "groupArrayIf(" + max_size + ")(" + expr + " , " + expr + " IS NOT NULL)";
    } else
        out = "groupArrayIf(" + expr + " , " + expr + " IS NOT NULL)";
    return true;
}

bool MakeListIf::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name,pos);
    ++pos;
    const auto predicate = getConvertedArgument(fn_name,pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name,pos);
        out = "groupArrayIf(" + max_size + ")(" + expr + " , " + predicate+ " )";
    } else
        out = "groupArrayIf(" + expr + " , " + predicate+ " )";
    return true;
}

bool MakeListWithNulls::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"groupArray"); //groupArray takes everything including NULLs
}

bool MakeSet::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name,pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name,pos);
        out = "groupUniqArray(" + max_size + ")(" + expr + ")";
    } else
        out = "groupUniqArray(" + expr + ")";    
    return true;
}

bool MakeSetIf::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name,pos);
    ++pos;
    const auto predicate = getConvertedArgument(fn_name,pos);
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name,pos);
        out = "groupUniqArrayIf(" + max_size + ")(" + expr + " , " + predicate+ " )";
    } else
        out = "groupUniqArrayIf(" + expr + " , " + predicate+ " )";
    return true;
}

bool Max::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"max");
}

bool MaxIf::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"maxIf");
}

bool Min::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"min");
}

bool MinIf::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"minIf");
}

bool Percentiles::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    
    ++pos;
    String column_name = getConvertedArgument(fn_name,pos);
    trim(column_name);
    String expr;
    String value;
    String value_in_column;
    while(pos->type != TokenType::ClosingRoundBracket)
    {
        if(pos->type != TokenType::Comma){
            value = String(pos->begin, pos->end);
            value_in_column = "";

            for(size_t i = 0; i < value.size(); i++)
            {
                if(value[i] == '.')
                    value_in_column += '_';
                else
                    value_in_column += value[i];
            }
            expr = expr + "quantile( " + value + "/100)(" + column_name + ") AS percentile_" + column_name + "_" + value_in_column;
            ++pos;
            if(pos->type != TokenType::ClosingRoundBracket)
                expr += ", ";
        }
        else
            ++pos;
    }
    out = expr;
    return true;
}

bool PercentilesArray::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name,pos);
    trim(column_name);
    String expr = "quantiles(";
    String value;
    while(pos->type != TokenType::ClosingRoundBracket)
    {
        if(pos->type != TokenType::Comma && String(pos->begin, pos->end) != "dynamic" 
            && pos->type != TokenType::OpeningRoundBracket && pos->type != TokenType::OpeningSquareBracket
            && pos->type != TokenType::ClosingSquareBracket){
            
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";

            if(pos->type != TokenType::Comma && pos->type != TokenType::OpeningRoundBracket && pos->type != TokenType::OpeningSquareBracket
            && pos->type != TokenType::ClosingSquareBracket)
                expr += ", ";
            ++pos;
        }
        else
        {
            ++pos;
        }

    }
    ++pos;
    if(pos->type != TokenType::ClosingRoundBracket)
        --pos;

    expr.pop_back();
    expr.pop_back();
    expr = expr + ")(" + column_name + ")";
    out = expr;
    return true;
}

bool Percentilesw::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name,pos);
    bucket_column.pop_back();

    ++pos;
    String frequency_column = getConvertedArgument(fn_name,pos);
    frequency_column.pop_back();

    String expr;
    String value;
    String value_in_column;

    while(pos->type != TokenType::ClosingRoundBracket)
    {
        if(pos->type != TokenType::Comma){
            value = String(pos->begin, pos->end);
            value_in_column = "";

            for(size_t i = 0; i < value.size(); i++)
            {
                if(value[i] == '.')
                    value_in_column += '_';
                else
                    value_in_column += value[i];
            }

            expr = expr + "quantileExactWeighted( " + value + "/100)(" + bucket_column + ","+frequency_column + ") AS percentile_" + bucket_column + "_" + value_in_column;
            ++pos;
            if(pos->type != TokenType::ClosingRoundBracket)
                expr += ", ";
        }
        else
            ++pos;
    }
    out = expr;
    return true;
}

bool PercentileswArray::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name,pos);
    bucket_column.pop_back();

    ++pos;
    String frequency_column = getConvertedArgument(fn_name,pos);
    frequency_column.pop_back();

    String expr = "quantilesExactWeighted(";
    String value;
    while(pos->type != TokenType::ClosingRoundBracket)
    {
        if(pos->type != TokenType::Comma && String(pos->begin, pos->end) != "dynamic" 
            && pos->type != TokenType::OpeningRoundBracket && pos->type != TokenType::OpeningSquareBracket
            && pos->type != TokenType::ClosingSquareBracket){
            
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";

            if(pos->type != TokenType::Comma && pos->type != TokenType::OpeningRoundBracket && pos->type != TokenType::OpeningSquareBracket
            && pos->type != TokenType::ClosingSquareBracket)
                expr += ", ";
            ++pos;
        }
        else
        {
            ++pos;
        }

    }
    ++pos;
    if(pos->type != TokenType::ClosingRoundBracket)
        --pos;

    expr.pop_back();
    expr.pop_back();
    expr = expr + ")("  + bucket_column + ","+frequency_column +  ")";
    out = expr;
    return true;
}

bool Stdev::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name,pos);
    out = "sqrt(varSamp(" + expr + "))";
    return true;
}

bool StdevIf::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name,pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const auto predicate = getConvertedArgument(fn_name,pos);
    out = "sqrt(varSampIf(" + expr + ", " + predicate + "))";
    return true;
}

bool Sum::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"sum");
}

bool SumIf::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"sumIf");
}

bool TakeAny::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool TakeAnyIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Variance::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool VarianceIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
