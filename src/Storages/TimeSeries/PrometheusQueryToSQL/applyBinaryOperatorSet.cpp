#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorSet.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

void checkArgumentTypesForSetBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    const SQLQueryPiece & left_argument,
    const SQLQueryPiece & right_argument,
    const ConverterContext & context)
{
    std::string_view operator_name = operator_node->operator_name;

    if (left_argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Binary operator '{}' expects two arguments of type {}, but expression {} has type {}",
                        operator_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(left_argument, context), left_argument.type);
    }

    if (right_argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Binary operator '{}' expects two arguments of type {}, but expression {} has type {}",
                        operator_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(right_argument, context), right_argument.type);
    }

    if (operator_node->group_left)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Binary operator '{}' doesn't allow group_left",
                        operator_name);
    }

    if (operator_node->group_right)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Binary operator '{}' doesn't allow group_right",
                        operator_name);
    }
}

ASTPtr makePresenceMask(ASTPtr values)
{
    auto lambda = makeASTFunction(
        "lambda",
        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")));

    return makeASTFunction(
        "groupBitOrForEach",
        makeASTFunction("arrayMap", std::move(lambda), std::move(values)));
}

}
