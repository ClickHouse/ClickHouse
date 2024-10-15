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
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SYNTAX_ERROR;
}

bool ArrayConcat::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "arrayConcat");
}

bool ArrayIif::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto conditions = getArgument(function_name, pos);
    const auto if_true = getArgument(function_name, pos);
    const auto if_false = getArgument(function_name, pos);

    out = std::format(
        "arrayMap(x -> multiIf(toTypeName(x.1) = 'String', null, toInt64(x.1) != 0, x.2, x.3), "
        "arrayZip({0}, arrayResize({1}, length({0}), null), arrayResize({2}, length({0}), null)))",
        conditions,
        if_true,
        if_false);

    return true;
}

bool ArrayIndexOf::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto array = getArgument(fn_name, pos);
    const auto needle = getArgument(fn_name, pos);
    out = "minus(indexOf(" + array + ", " + needle + "), 1)";

    return true;
}

bool ArrayLength::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "length");
}

bool ArrayReverse::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos);
    out = std::format("if(throwIf(not startsWith(toTypeName({0}), 'Array'), 'Only arrays are supported'), [], reverse({0}))", array);

    return true;
}

bool ArrayRotateLeft::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos);
    const auto count = getArgument(function_name, pos);
    out = std::format(
        "arrayMap(x -> {0}[moduloOrZero(x + length({0}) + moduloOrZero({1}, toInt64(length({0}))), length({0})) + 1], "
        "range(0, length({0})))",
        array,
        count);

    return true;
}

bool ArrayRotateRight::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos, ArgumentState::Raw);
    const auto count = getArgument(function_name, pos, ArgumentState::Raw);
    out = kqlCallToExpression("array_rotate_left", {array, "-1 * " + count}, pos.max_depth, pos.max_backtracks);

    return true;
}

bool ArrayShiftLeft::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos);
    const auto count = getArgument(function_name, pos);
    const auto fill = getOptionalArgument(function_name, pos);
    out = std::format(
        "arrayResize(if({1} > 0, arraySlice({0}, {1} + 1), arrayConcat(arrayWithConstant(abs({1}), fill_value_{3}), {0})), "
        "length({0}), if(isNull({2}) and (extract(toTypeName({0}), 'Array\\((.*)\\)') as element_type_{3}) = 'String', "
        "defaultValueOfTypeName(if(element_type_{3} = 'Nothing', 'Nullable(Nothing)', element_type_{3})), {2}) as fill_value_{3})",
        array,
        count,
        fill ? *fill : "null",
        generateUniqueIdentifier());

    return true;
}

bool ArrayShiftRight::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos, ArgumentState::Raw);
    const auto count = getArgument(function_name, pos, ArgumentState::Raw);
    const auto fill = getOptionalArgument(function_name, pos, ArgumentState::Raw);

    const auto negated_count = "-1 * " + count;
    out = kqlCallToExpression(
        "array_shift_left",
        fill ? std::initializer_list<std::string_view>{array, negated_count, *fill}
             : std::initializer_list<std::string_view>{array, negated_count},
        pos.max_depth, pos.max_backtracks);

    return true;
}

bool ArraySlice::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos);
    const auto start = getArgument(function_name, pos);
    const auto end = getArgument(function_name, pos);

    out = std::format(
        "arraySlice({0}, plus(1, if({1} >= 0, {1}, arrayMax([-length({0}), {1}]) + length({0}))) as offset_{3}, "
        "                plus(1, if({2} >= 0, {2}, arrayMax([-length({0}), {2}]) + length({0}))) - offset_{3} + 1)",
        array,
        start,
        end,
        generateUniqueIdentifier());

    return true;
}

bool ArraySortAsc::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_array_sort_asc");
}

bool ArraySortDesc::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "kql_array_sort_desc");
}

bool ArraySplit::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto array = getArgument(function_name, pos);
    const auto indices = getArgument(function_name, pos);

    out = std::format(
        "if(empty(arrayMap(x -> if(x >= 0, x, arrayMax([0, x + length({0})::Int64])), flatten([{1}])) as indices_{2}), [{0}], "
        "arrayConcat([arraySlice({0}, 1, indices_{2}[1])], arrayMap(i -> arraySlice({0}, indices_{2}[i] + 1, "
        "if(i = length(indices_{2}), length({0})::Int64, indices_{2}[i + 1]::Int64) - indices_{2}[i]), "
        "range(1, length(indices_{2}) + 1))))",
        array,
        indices,
        generateUniqueIdentifier());

    return true;
}

bool ArraySum::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "arraySum");
}

bool BagKeys::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool BagMerge::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool BagRemoveKeys::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool JaccardIndex::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto rhs = getArgument(function_name, pos, ArgumentState::Raw);
    out = std::format(
        "divide(length({0}), length({1}))",
        kqlCallToExpression("set_intersect", {lhs, rhs}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("set_union", {lhs, rhs}, pos.max_depth, pos.max_backtracks));

    return true;
}

bool Pack::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool PackAll::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool PackArray::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "array", {1, Interval::max_bound});
}

bool Repeat::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    String value = getArgument(function_name, pos);
    String count = getArgument(function_name, pos);

    value.erase(remove(value.begin(), value.end(), ' '), value.end());
    count.erase(remove(count.begin(), count.end(), ' '), count.end());

    if (count.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "number of arguments do not match in function: {}", function_name);
    out = "if(" + count + " < 0, [NULL], " + std::format("arrayWithConstant(abs({1}), {0}))", value, count);

    return true;
}

bool SetDifference::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = std::invoke(
        [&function_name, &pos]
        {
            std::vector<String> arrays{getArgument(function_name, pos, ArgumentState::Raw)};
            while (auto next_array = getOptionalArgument(function_name, pos, ArgumentState::Raw))
                arrays.push_back(*next_array);

            return kqlCallToExpression("set_union", std::vector<std::string_view>(arrays.cbegin(), arrays.cend()), pos.max_depth, pos.max_backtracks);
        });

    out = std::format("arrayFilter(x -> not has({1}, x), arrayDistinct({0}))", lhs, rhs);

    return true;
}

bool SetHasElement::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "has");
}

bool SetIntersect::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "arrayIntersect");
}

bool SetUnion::convertImpl(String & out, IParser::Pos & pos)
{
    if (!directMapping(out, pos, "arrayConcat"))
        return false;

    out = std::format("arrayDistinct({0})", out);

    return true;
}

bool TreePath::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Zip::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto arguments = std::invoke(
        [&function_name, &pos]
        {
            std::vector<String> result;
            while (auto argument = getOptionalArgument(function_name, pos))
                result.push_back(std::move(*argument));

            return result;
        });

    if (const auto size = arguments.size(); size < 2 || size > 16)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Between 2 and 16 arguments are expected, but {} were provided", size);

    const auto unique_identifier = generateUniqueIdentifier();
    const auto resized_arguments = std::invoke(
        [&arguments, &unique_identifier]
        {
            String lengths;
            for (int i = 0; i < std::ssize(arguments); ++i)
            {
                lengths.append(i > 0 ? ", " : "");
                lengths.append(std::format(
                    "length(if(match(toTypeName({0}), 'Array\\(Nullable\\(.*\\)\\)'), {0}, "
                    "cast({0}, concat('Array(Nullable(', extract(toTypeName({0}), 'Array\\((.*)\\)'), '))'))) as arg{1}_{2})",
                    arguments[i],
                    i,
                    unique_identifier));
            }

            auto result = std::format("arrayResize(arg0_{1}, arrayMax([{0}]) as max_length_{1}, null)", lengths, unique_identifier);
            for (int i = 1; i < std::ssize(arguments); ++i)
                result.append(std::format(", arrayResize(arg{0}_{1}, max_length_{1}, null)", i, unique_identifier));

            return result;
        });

    out = std::format("arrayMap(t -> [untuple(t)], arrayZip({0}))", resized_arguments);

    return true;
}
}
