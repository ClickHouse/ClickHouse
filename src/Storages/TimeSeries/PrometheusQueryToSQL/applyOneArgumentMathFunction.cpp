#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentMathFunction.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <boost/math/special_functions/sign.hpp>
#include <numbers>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a math function.
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

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
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context);

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        chassert(args.size() == 1);
        ASTPtr x = std::move(args[0]);
        return makeASTFunction(impl_info->ch_function_name, std::move(x));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
