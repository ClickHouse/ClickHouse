#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>

#include <Common/Exception.h>
#include <Functions/DivisionUtils.h>
#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>
#include <cmath>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        const SQLQueryPiece & left_argument,
        const SQLQueryPiece & right_argument,
        const ConverterContext & context)
    {
        std::string_view operator_name = operator_node->operator_name;

        if ((left_argument.type != ResultType::SCALAR) && (left_argument.type != ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLText(left_argument, context), left_argument.type);
        }

        if ((right_argument.type != ResultType::SCALAR) && (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLText(right_argument, context), right_argument.type);
        }

        if (operator_node->bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' doesn't allow bool modifier",
                            operator_name);
        }

        if ((left_argument.type != ResultType::INSTANT_VECTOR) || (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            if (operator_node->group_left || operator_node->group_right)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Binary operator '{}' with the group modifier expects two arguments of type {}, got {} and {}",
                                operator_name, ResultType::INSTANT_VECTOR, left_argument.type, right_argument.type);
            }
        }
    }

    /// Computes `x % y` the same way as the SQL expression built by applyMathBinaryOperatorToAST().
    Float64 moduloOfScalars(Float64 x, Float64 y)
    {
        if (std::isinf(y) && std::isfinite(x))
            return x;
        return ModuloImpl<Float64, Float64>::apply(x, y);
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
        Float64 (*apply_to_scalars)(Float64, Float64);
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"+",     {"plus", [](Float64 x, Float64 y) { return x + y; }}},
            {"-",     {"minus", [](Float64 x, Float64 y) { return x - y; }}},
            {"*",     {"multiply", [](Float64 x, Float64 y) { return x * y; }}},
            {"/",     {"divide", [](Float64 x, Float64 y) { return x / y; }}},
            {"%",     {"modulo", moduloOfScalars}},
            {"^",     {"pow", [](Float64 x, Float64 y) { return std::pow(x, y); }}},
            {"atan2", {"atan2", [](Float64 x, Float64 y) { return std::atan2(x, y); }}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}

bool isMathBinaryOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


ASTPtr applyMathBinaryOperatorToAST(std::string_view operator_name, ASTPtr x, ASTPtr y)
{
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    if (operator_name != "%")
        return makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y));

    ASTPtr result = makeASTFunction(impl_info->ch_function_name, x->clone(), y->clone());

    return makeASTFunction(
        "if",
        makeASTFunction(
            "and",
            makeASTFunction("isInfinite", y->clone()),
            makeASTFunction("isFinite", x->clone())),
        std::move(x),
        std::move(result));
}


SQLQueryPiece applyMathBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    const auto & operator_name = operator_node->operator_name;

    /// An operator on two constant scalars is computed here, so the result is a constant too.
    /// Vector matching is left to applySimpleBinaryOperator(), which rejects it for scalars.
    if ((left_argument.store_method == StoreMethod::CONST_SCALAR) && (right_argument.store_method == StoreMethod::CONST_SCALAR)
        && (operator_node->result_type == ResultType::SCALAR) && operator_node->labels.empty())
    {
        auto res = left_argument;
        res.node = operator_node;
        res.scalar_value = getImplInfo(operator_name)->apply_to_scalars(left_argument.scalar_value, right_argument.scalar_value);
        return res;
    }

    auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
    {
        return applyMathBinaryOperatorToAST(operator_name, std::move(x), std::move(y));
    };

    return applySimpleBinaryOperator(
        operator_node,
        std::move(left_argument),
        std::move(right_argument),
        context,
        apply_function_to_ast,
        /* drop_metric_name = */ true,
        /* allow_grouping_modifier_copy_metric_name = */ true);
}

}
