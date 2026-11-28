#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentMathFunction.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <boost/math/special_functions/sign.hpp>
#include <numbers>
#include <unordered_map>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    struct ImplInfo
    {
        std::string_view ch_function_name;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"abs",   {"abs"}},
            {"sgn",   {"sign"}},
            {"floor", {"floor"}},
            {"ceil",  {"ceil"}},
            {"sqrt",  {"sqrt"}},
            {"exp",   {"exp"}},
            {"ln",    {"log"}},
            {"log2",  {"log2"}},
            {"log10", {"log10"}},
            {"rad",   {"radians"}},
            {"deg",   {"degrees"}},
            {"sin",   {"sin"}},
            {"cos",   {"cos"}},
            {"tan",   {"tan"}},
            {"asin",  {"asin"}},
            {"acos",  {"acos"}},
            {"atan",  {"atan"}},
            {"sinh",  {"sinh"}},
            {"cosh",  {"cosh"}},
            {"tanh",  {"tanh"}},
            {"asinh", {"asinh"}},
            {"acosh", {"acosh"}},
            {"atanh", {"atanh"}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isOneArgumentMathFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyOneArgumentMathFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkNumberOfArguments(function_node, arguments, 1);
    checkArgumentType(function_node, arguments, 0, ResultType::INSTANT_VECTOR, context);

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        chassert(args.size() == 1);
        ASTPtr x = std::move(args[0]);

        if (function_name == "sgn")
        {
            ASTPtr zero = timeSeriesScalarToAST(0);
            return makeASTFunction(
                "multiIf",
                makeASTFunction("less", x->clone(), zero->clone()),
                timeSeriesScalarToAST(-1),
                makeASTFunction("greater", x->clone(), zero->clone()),
                timeSeriesScalarToAST(1),
                std::move(x));
        }

        return makeASTFunction(impl_info->ch_function_name, std::move(x));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
