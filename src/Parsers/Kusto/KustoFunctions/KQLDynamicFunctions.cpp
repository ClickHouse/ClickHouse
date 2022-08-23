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

#include <format>

namespace DB
{

bool ArrayConcat::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "arrayConcat");
}

bool ArrayIif::convertImpl(String & out,IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto conditions = getArgument(function_name, pos);
    const auto if_true = getArgument(function_name, pos);
    const auto if_false = getArgument(function_name, pos);

    out = std::format(
        "arrayMap(x -> if(x.1 != 0, x.2, x.3), arrayZip({0}, arrayResize({1}, length({0}), null), arrayResize({2}, length({0}), null)))",
        conditions,
        if_true,
        if_false);

    return true;
}

bool ArrayIndexOf::convertImpl(String & out,IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto array = getArgument(fn_name, pos);
    const auto needle = getArgument(fn_name, pos);
    out = "minus(indexOf(" + array + ", " + needle + "), 1)";

    return true;
}

bool ArrayLength::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "length");
}

bool ArrayReverse::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool ArrayRotateLeft::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool ArrayRotateRight::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool ArrayShiftLeft::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool ArrayShiftRight::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool ArraySlice::convertImpl(String & out,IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos);
    const auto start = getArgument(function_name, pos);
    const auto end = getArgument(function_name, pos);

    out = std::format(
        "arraySlice({0}, plus(1, if({1} >= 0, {1}, toInt64(max2(-length({0}), {1})) + length({0}))) as offset_{3}, "
        "                plus(1, if({2} >= 0, {2}, toInt64(max2(-length({0}), {2})) + length({0}))) - offset_{3} + 1)",
        array,
        start,
        end,
        generateUniqueIdentifier());

    return true;
}

bool ArraySortAsc::convertImpl(String & out,IParser::Pos & pos)
{
    out = ArraySortHelper(out, pos, true);
    if(out == "false")
        return false;
    return true;
}

bool ArraySortDesc::convertImpl(String & out,IParser::Pos & pos)
{
    out = ArraySortHelper(out, pos, false);
    if(out == "false")
        return false;
    return true;
}

bool ArraySplit::convertImpl(String & out,IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
    return false;

    const auto array = getArgument(function_name, pos);
    const auto indices = getArgument(function_name, pos);

    out = std::format(
        "if(empty(arrayMap(x -> if(x >= 0, x, toInt64(max2(0, x + length({0})))), flatten([{1}])) as indices_{2}), [{0}], "
        "arrayConcat([arraySlice({0}, 1, indices_{2}[1])], arrayMap(i -> arraySlice({0}, indices_{2}[i] + 1, "
        "if(i = length(indices_{2}), length({0})::Int64, indices_{2}[i + 1]::Int64) - indices_{2}[i]), "
        "range(1, length(indices_{2}) + 1))))",
        array,
        indices,
        generateUniqueIdentifier());

    return true;
}

bool ArraySum::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "arraySum");
}

bool BagKeys::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool BagMerge::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool BagRemoveKeys::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool JaccardIndex::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Pack::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool PackAll::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool PackArray::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Repeat::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SetDifference::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SetHasElement::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SetIntersect::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SetUnion::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool TreePath::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Zip::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}
}
