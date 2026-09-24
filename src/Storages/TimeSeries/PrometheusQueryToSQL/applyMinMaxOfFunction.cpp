#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMinMaxOfFunction.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <limits>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 2, arguments.size());
        }

        for (size_t i = 0; i != arguments.size(); ++i)
        {
            if (arguments[i].type != ResultType::SCALAR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument #{} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::SCALAR,
                                getPromQLText(arguments[i], context), arguments[i].type);
            }
        }
    }
}


bool isMinMaxOfFunction(std::string_view function_name)
{
    return function_name == "min_of" || function_name == "max_of";
}


SQLQueryPiece applyMinMaxOfFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);
    bool is_min = function_node->function_name == "min_of";

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        chassert(args.size() == 2);
        const auto & x = args[0];
        const auto & y = args[1];
        auto zero = timeSeriesScalarToAST(0);
        auto infinity = timeSeriesScalarToAST(
            is_min ? -std::numeric_limits<Float64>::infinity() : std::numeric_limits<Float64>::infinity());

        /// Prometheus uses Go's `math.Min` and `math.Max`: the extremal infinity takes precedence over NaN.
        /// For zero ties, the sign of `1 / x` selects -0 for `min_of` and +0 for `max_of` in either argument order.
        auto zero_result = makeASTFunction("if",
            makeASTFunction("less",
                makeASTFunction("divide", timeSeriesScalarToAST(1), x->clone()), zero->clone()),
            is_min ? x->clone() : y->clone(),
            is_min ? y->clone() : x->clone());

        return makeASTFunction("multiIf",
            makeASTFunction("or",
                makeASTFunction("equals", x->clone(), infinity->clone()),
                makeASTFunction("equals", y->clone(), infinity->clone())),
            infinity->clone(),
            makeASTFunction("or", makeASTFunction("isNaN", x->clone()), makeASTFunction("isNaN", y->clone())),
            timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN()),
            makeASTFunction("and",
                makeASTFunction("equals", x->clone(), zero->clone()),
                makeASTFunction("equals", y->clone(), zero->clone())),
            std::move(zero_result),
            makeASTFunction(is_min ? "least" : "greatest", x->clone(), y->clone()));
    };

    return applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
}

}
