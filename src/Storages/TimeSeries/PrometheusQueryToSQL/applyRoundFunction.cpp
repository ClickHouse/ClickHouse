#include <Storages/TimeSeries/PrometheusQueryToSQL/applyRoundFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>


namespace DB::PrometheusQueryToSQL
{

bool isRoundFunction(std::string_view function_name)
{
    return function_name == "round";
}


SQLQueryPiece applyRoundFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkNumberOfArgumentsBetween(function_node, arguments, 1, 2);
    checkArgumentType(function_node, arguments, 0, ResultType::INSTANT_VECTOR, context);
    if (arguments.size() == 2)
        checkArgumentType(function_node, arguments, 1, ResultType::SCALAR, context);

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
