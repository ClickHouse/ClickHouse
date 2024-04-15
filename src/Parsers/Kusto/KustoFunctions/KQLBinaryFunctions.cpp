#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/IKQLParser.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>

#include <format>

namespace DB
{

bool BinaryAnd::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    out = std::format("bitAnd(cast({0}, 'Int64'), cast({1}, 'Int64'))", lhs, rhs);
    return true;
}

bool BinaryNot::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto value = getArgument(function_name, pos);
    out = std::format("bitNot(cast({0}, 'Int64'))", value);
    return true;
}

bool BinaryOr::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    out = std::format("bitOr(cast({0}, 'Int64'), cast({1}, 'Int64'))", lhs, rhs);
    return true;
}

bool BinaryShiftLeft::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto value = getArgument(function_name, pos);
    const auto count = getArgument(function_name, pos);
    out = std::format("if({1} < 0, null, bitShiftLeft(cast({0}, 'Int64'), {1}))", value, count);
    return true;
}

bool BinaryShiftRight::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto value = getArgument(function_name, pos);
    const auto count = getArgument(function_name, pos);
    out = std::format("if({1} < 0, null, bitShiftRight(cast({0}, 'Int64'), {1}))", value, count);
    return true;
}

bool BinaryXor::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    out = std::format("bitXor(cast({0}, 'Int64'), cast({1}, 'Int64'))", lhs, rhs);
    return true;
}

bool BitsetCountOnes::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "bitCount");
}

}
