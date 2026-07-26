#include <Storages/TimeSeries/PrometheusQueryToSQL/applyRoundFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <limits>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the round() function.
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if ((arguments.size() != 1) && (arguments.size() != 2))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 1 or 2 arguments, but was called with {} arguments",
                            function_name, arguments.size());
        }

        if (arguments[0].type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(arguments[0], context), arguments[0].type);
        }

        if ((arguments.size() == 2) && (arguments[1].type != ResultType::SCALAR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects second argument of type {}, but expression {} has type {}",
                            function_name, ResultType::SCALAR,
                            getPromQLText(arguments[1], context), arguments[1].type);
        }
    }
}


bool isRoundFunction(std::string_view function_name)
{
    return function_name == "round";
}


SQLQueryPiece applyRoundFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    /// PromQL: scalar() returns NaN if its argument is an empty vector, so a `to_nearest` argument which is known
    /// to be empty at this point (e.g. scalar(clamp(v, 1, -1))) means to_nearest == NaN, and the result must
    /// consist of NaN values. We convert such an argument to a NaN constant here so that the calculation below
    /// applies to it (rounding to the nearest multiple of NaN produces NaN).
    /// (An empty first argument still makes the result empty - see applySimpleFunction()).
    if ((arguments.size() == 2) && (arguments[1].store_method == StoreMethod::EMPTY))
    {
        auto & argument = arguments[1];
        auto node_range = context.node_range_getter.get(argument.node);
        if (!node_range.empty())
        {
            argument.store_method = StoreMethod::CONST_SCALAR;
            argument.scalar_value = std::numeric_limits<Float64>::quiet_NaN();
            argument.start_time = node_range.start_time;
            argument.end_time = node_range.end_time;
            argument.step = node_range.step;
        }
    }

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        chassert((args.size() == 1) || (args.size() == 2));
        ASTPtr x = std::move(args[0]);

        /// PromQL rounds ties up (unlike ClickHouse's round() which uses banker's rounding),
        /// so round(x) is calculated as floor(x + 0.5).
        if (args.size() == 1)
            return makeASTFunction("floor", makeASTFunction("plus", std::move(x), make_intrusive<ASTLiteral>(0.5)));

        /// round(x, to_nearest) is calculated as floor(x * inv + 0.5) / inv, where inv = 1 / to_nearest.
        /// Prometheus uses the inverse of to_nearest because it seems to cause fewer floating point
        /// accuracy issues, and we replicate its arithmetic exactly to produce the same results.
        ASTPtr inv = makeASTFunction("divide", make_intrusive<ASTLiteral>(1.0), std::move(args[1]));
        return makeASTFunction("divide",
            makeASTFunction("floor",
                makeASTFunction("plus",
                    makeASTFunction("multiply", std::move(x), inv->clone()),
                    make_intrusive<ASTLiteral>(0.5))),
            std::move(inv));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
