#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentMathFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
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
    using TransformASTFunc = ASTPtr (*)(ASTs args);

    struct ImplInfo
    {
        std::string_view ch_function_name;
        TransformASTFunc transform_ast = nullptr;
    };

    bool isRoundFunction(std::string_view function_name)
    {
        return function_name == "round";
    }

    /// Checks if the types of the specified arguments are valid for a math function.
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        /// `round` optionally accepts a second `to_nearest` argument; every other math function is unary.
        const bool round_function = isRoundFunction(function_name);
        const bool valid_arity = arguments.size() == 1 || (round_function && arguments.size() == 2);

        if (!valid_arity)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, round_function ? "1 or 2" : "1", arguments.size());
        }

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }

        if (isRoundFunction(function_name) && arguments.size() == 2 && arguments[1].type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects second argument of type {}, but expression {} has type {}",
                            function_name, ResultType::SCALAR,
                            getPromQLText(arguments[1], context), arguments[1].type);
        }
    }

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
            {"round",
             {
                 "",
                 [](ASTs args) -> ASTPtr
                 {
                     /// PromQL round(v, to_nearest=1) resolves ties by rounding up.
                     /// Use the same reciprocal formula as Prometheus because decimal
                     /// cases such as round(0.15, 0.1) can differ from v / to_nearest.
                     ASTPtr value = std::move(args[0]);
                     ASTPtr to_nearest = (args.size() == 2) ? std::move(args[1]) : make_intrusive<ASTLiteral>(1.0);
                     ASTPtr to_nearest_inverse = makeASTFunction("divide", make_intrusive<ASTLiteral>(1.0), std::move(to_nearest));
                     ASTPtr rounded = makeASTFunction(
                         "divide",
                         makeASTFunction(
                             "floor",
                             makeASTFunction(
                                 "plus",
                                 makeASTFunction("multiply", value->clone(), to_nearest_inverse->clone()),
                                 make_intrusive<ASTLiteral>(0.5))),
                         std::move(to_nearest_inverse));

                     /// A Prometheus stale marker is a `NaN` with the exact payload 0x7ff0000000000002, and it is
                     /// recognized by that bit pattern only at finalization. Arithmetic quiets the payload into an
                     /// ordinary `NaN`, which would make a stale sample survive as a `NaN` value instead of being
                     /// dropped, so stale samples are passed through unchanged here.
                     return makeASTFunction(
                         "if",
                         makeASTFunction(
                             "equals",
                             makeASTFunction("reinterpretAsUInt64", makeASTFunction("assumeNotNull", value->clone())),
                             make_intrusive<ASTLiteral>(0x7ff0000000000002ULL)),
                         value->clone(),
                         std::move(rounded));
                 },
             }},
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
        if (impl_info->transform_ast)
            return impl_info->transform_ast(std::move(args));

        chassert(args.size() == 1);
        return makeASTFunction(impl_info->ch_function_name, std::move(args[0]));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
