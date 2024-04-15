#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/IKQLParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool ArgMax::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "argMax");
}

bool ArgMin::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "argMin");
}

bool Avg::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "avg");
}

bool AvgIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "avgIf");
}

bool BinaryAllAnd::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "groupBitAnd");
}

bool BinaryAllOr::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "groupBitOr");
}

bool BinaryAllXor::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "groupBitXor");
}

bool BuildSchema::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool Count::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "count");
}

bool CountIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "countIf");
}

bool DCount::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name, pos);

    out = "count(DISTINCT " + value + ")";
    return true;
}

bool DCountIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    ++pos;
    String condition = getConvertedArgument(fn_name, pos);
    out = "countIf (DISTINCT " + value + ", " + condition + ")";
    return true;
}

bool MakeBag::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool MakeBagIf::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool MakeList::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type == KQLTokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupArrayIf(" + max_size + ")(" + expr + " , " + expr + " IS NOT NULL)";
    }
    else
        out = "groupArrayIf(" + expr + " , " + expr + " IS NOT NULL)";
    return true;
}

bool MakeListIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    if (pos->type == KQLTokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupArrayIf(" + max_size + ")(" + expr + " , " + predicate + " )";
    }
    else
        out = "groupArrayIf(" + expr + " , " + predicate + " )";
    return true;
}

bool MakeListWithNulls::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto column_name = getConvertedArgument(fn_name, pos);
    out = "arrayConcat(groupArray(" + column_name + "), arrayMap(x -> null, range(0, toUInt32(count(*)-length(  groupArray(" + column_name
        + "))),1)))";
    return true;
}

bool MakeSet::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type == KQLTokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupUniqArray(" + max_size + ")(" + expr + ")";
    }
    else
        out = "groupUniqArray(" + expr + ")";
    return true;
}

bool MakeSetIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    if (pos->type == KQLTokenType::Comma)
    {
        ++pos;
        const auto max_size = getConvertedArgument(fn_name, pos);
        out = "groupUniqArrayIf(" + max_size + ")(" + expr + " , " + predicate + " )";
    }
    else
        out = "groupUniqArrayIf(" + expr + " , " + predicate + " )";
    return true;
}

bool Max::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "max");
}

bool MaxIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "maxIf");
}

bool Min::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "min");
}

bool MinIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "minIf");
}

bool Percentile::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name, pos);
    trim(column_name);

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    trim(value);

    out = "quantile(" + value + "/100)(" + column_name + ")";
    return true;
}

bool Percentilew::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name, pos);
    trim(bucket_column);

    ++pos;
    String frequency_column = getConvertedArgument(fn_name, pos);
    trim(frequency_column);

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    trim(value);

    out = "quantileExactWeighted(" + value + "/100)(" + bucket_column + "," + frequency_column + ")";
    return true;
}

bool Percentiles::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name, pos);
    trim(column_name);
    String expr = "quantiles(";
    String value;
    while (pos->type != KQLTokenType::ClosingRoundBracket)
    {
        if (pos->type != KQLTokenType::Comma)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";
            ++pos;
            if (pos->type != KQLTokenType::ClosingRoundBracket)
                expr += ", ";
        }
        else
            ++pos;
    }
    out = expr + ")(" + column_name + ")";
    return true;
}

bool PercentilesArray::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String column_name = getConvertedArgument(fn_name, pos);
    trim(column_name);
    String expr = "quantiles(";
    String value;
    while (pos->type != KQLTokenType::ClosingRoundBracket)
    {
        if (pos->type != KQLTokenType::Comma && String(pos->begin, pos->end) != "dynamic" && pos->type != KQLTokenType::OpeningRoundBracket
            && pos->type != KQLTokenType::OpeningSquareBracket && pos->type != KQLTokenType::ClosingSquareBracket)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";

            if (pos->type != KQLTokenType::Comma && pos->type != KQLTokenType::OpeningRoundBracket && pos->type != KQLTokenType::OpeningSquareBracket
                && pos->type != KQLTokenType::ClosingSquareBracket)
                expr += ", ";
            ++pos;
        }
        else
        {
            ++pos;
        }
    }
    ++pos;
    if (pos->type != KQLTokenType::ClosingRoundBracket)
        --pos;

    expr.pop_back();
    expr.pop_back();
    expr = expr + ")(" + column_name + ")";
    out = expr;
    return true;
}

bool Percentilesw::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name, pos);
    trim(bucket_column);

    ++pos;
    String frequency_column = getConvertedArgument(fn_name, pos);
    trim(frequency_column);

    String expr = "quantilesExactWeighted(";
    String value;

    while (pos->type != KQLTokenType::ClosingRoundBracket)
    {
        if (pos->type != KQLTokenType::Comma)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";
            ++pos;
            if (pos->type != KQLTokenType::ClosingRoundBracket)
                expr += ", ";
        }
        else
            ++pos;
    }
    expr = expr + ")(" + bucket_column + "," + frequency_column + ")";
    out = expr;
    return true;
}

bool PercentileswArray::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String bucket_column = getConvertedArgument(fn_name, pos);
    trim(bucket_column);

    ++pos;
    String frequency_column = getConvertedArgument(fn_name, pos);
    trim(frequency_column);

    String expr = "quantilesExactWeighted(";
    String value;
    while (pos->type != KQLTokenType::ClosingRoundBracket)
    {
        if (pos->type != KQLTokenType::Comma && String(pos->begin, pos->end) != "dynamic" && pos->type != KQLTokenType::OpeningRoundBracket
            && pos->type != KQLTokenType::OpeningSquareBracket && pos->type != KQLTokenType::ClosingSquareBracket)
        {
            value = String(pos->begin, pos->end);
            expr = expr + value + "/100";

            if (pos->type != KQLTokenType::Comma && pos->type != KQLTokenType::OpeningRoundBracket && pos->type != KQLTokenType::OpeningSquareBracket
                && pos->type != KQLTokenType::ClosingSquareBracket)
                expr += ", ";
            ++pos;
        }
        else
        {
            ++pos;
        }
    }
    ++pos;
    if (pos->type != KQLTokenType::ClosingRoundBracket)
        --pos;

    expr.pop_back();
    expr.pop_back();
    expr = expr + ")(" + bucket_column + "," + frequency_column + ")";
    out = expr;
    return true;
}

bool Stdev::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    out = "sqrt(varSamp(" + expr + "))";
    return true;
}

bool StdevIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    const auto expr = getConvertedArgument(fn_name, pos);
    if (pos->type != KQLTokenType::Comma)
        return false;

    ++pos;
    const auto predicate = getConvertedArgument(fn_name, pos);
    out = "sqrt(varSampIf(" + expr + ", " + predicate + "))";
    return true;
}

bool Sum::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "sum");
}

bool SumIf::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "sumIf");
}

bool TakeAny::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool TakeAnyIf::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool Variance::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool VarianceIf::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}
}
