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

    /// Most entries are direct ClickHouse calls, but PromQL round() needs an expression
    /// to preserve its default nearest-1 and tie-round-up semantics.
    using TransformASTFunc = ASTPtr (*)(ASTPtr x);

    struct ImplInfo
    {
        TransformASTFunc transform_ast;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"abs",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("abs", std::move(x)); }}},
            {"sgn",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("sign", std::move(x)); }}},
            {"floor", {[](ASTPtr x) -> ASTPtr { return makeASTFunction("floor", std::move(x)); }}},
            {"ceil",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("ceil", std::move(x)); }}},
            {"sqrt",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("sqrt", std::move(x)); }}},
            {"exp",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("exp", std::move(x)); }}},
            {"ln",    {[](ASTPtr x) -> ASTPtr { return makeASTFunction("log", std::move(x)); }}},
            {"log2",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("log2", std::move(x)); }}},
            {"log10", {[](ASTPtr x) -> ASTPtr { return makeASTFunction("log10", std::move(x)); }}},
            {"rad",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("radians", std::move(x)); }}},
            {"deg",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("degrees", std::move(x)); }}},
            {"sin",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("sin", std::move(x)); }}},
            {"cos",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("cos", std::move(x)); }}},
            {"tan",   {[](ASTPtr x) -> ASTPtr { return makeASTFunction("tan", std::move(x)); }}},
            {"asin",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("asin", std::move(x)); }}},
            {"acos",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("acos", std::move(x)); }}},
            {"atan",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("atan", std::move(x)); }}},
            {"sinh",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("sinh", std::move(x)); }}},
            {"cosh",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("cosh", std::move(x)); }}},
            {"tanh",  {[](ASTPtr x) -> ASTPtr { return makeASTFunction("tanh", std::move(x)); }}},
            {"asinh", {[](ASTPtr x) -> ASTPtr { return makeASTFunction("asinh", std::move(x)); }}},
            {"acosh", {[](ASTPtr x) -> ASTPtr { return makeASTFunction("acosh", std::move(x)); }}},
            {"atanh", {[](ASTPtr x) -> ASTPtr { return makeASTFunction("atanh", std::move(x)); }}},
            {"round",
             {
                 [](ASTPtr x) -> ASTPtr
                 {
                     /// PromQL round() defaults to nearest 1 and resolves ties by rounding up.
                     return makeASTFunction("floor", makeASTFunction("plus", std::move(x), make_intrusive<ASTLiteral>(0.5)));
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
        chassert(args.size() == 1);
        ASTPtr x = std::move(args[0]);
        return impl_info->transform_ast(std::move(x));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
