#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMinMaxOfFunction.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <limits>


namespace DB::PrometheusQueryToSQL
{

bool isMinMaxOfFunction(std::string_view function_name)
{
    return function_name == "min_of" || function_name == "max_of";
}


SQLQueryPiece applyMinMaxOfFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkNumberOfArguments(function_node, arguments, 2);
    for (size_t i = 0; i != arguments.size(); ++i)
        checkArgumentType(function_node, arguments, i, ResultType::SCALAR, context);
    bool is_min = function_node->function_name == "min_of";

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        chassert(args.size() == 2);
        const auto & x = args[0];
        const auto & y = args[1];
        return makeASTFunction("if",
            makeASTFunction("or", makeASTFunction("isNaN", x->clone()), makeASTFunction("isNaN", y->clone())),
            timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN()),
            makeASTFunction(is_min ? "least" : "greatest", x->clone(), y->clone()));
    };

    return applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
}

}
