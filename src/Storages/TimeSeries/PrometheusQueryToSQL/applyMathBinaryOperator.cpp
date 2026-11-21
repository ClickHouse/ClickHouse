#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>
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
        const PQT::BinaryOperator * operator_node,
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

    struct ImplInfo
    {
        std::string_view ch_function_name;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"+",     {"plus"}},
            {"-",     {"minus"}},
            {"*",     {"multiply"}},
            {"/",     {"divide"}},
            {"%",     {"modulo"}},
            {"^",     {"pow"}},
            {"atan2", {"atan2"}},
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


SQLQueryPiece applyMathBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
    {
        return makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y));
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
