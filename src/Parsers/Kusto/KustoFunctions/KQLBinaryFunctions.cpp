#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/ParserSetQuery.h>

#include <format>

namespace DB
{

bool BinaryAnd::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    out = std::format("bitAnd(cast({0}, 'Int64'), cast({1}, 'Int64'))", lhs, rhs);
    return true;
}

bool BinaryNot::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto value = getArgument(function_name, pos);
    out = std::format("bitNot(cast({0}, 'Int64'))", value);
    return true;
}

bool BinaryOr::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    out = std::format("bitOr(cast({0}, 'Int64'), cast({1}, 'Int64'))", lhs, rhs);
    return true;
}

bool BinaryShiftLeft::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto value = getArgument(function_name, pos);
    const auto count = getArgument(function_name, pos);
    out = std::format("if({1} < 0, null, bitShiftLeft(cast({0}, 'Int64'), {1}))", value, count);
    return true;
}

bool BinaryShiftRight::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto value = getArgument(function_name, pos);
    const auto count = getArgument(function_name, pos);
    out = std::format("if({1} < 0, null, bitShiftRight(cast({0}, 'Int64'), {1}))", value, count);
    return true;
}

bool BinaryXor::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    out = std::format("bitXor(cast({0}, 'Int64'), cast({1}, 'Int64'))", lhs, rhs);
    return true;
}

bool BitsetCountOnes::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "bitCount");
}

}
