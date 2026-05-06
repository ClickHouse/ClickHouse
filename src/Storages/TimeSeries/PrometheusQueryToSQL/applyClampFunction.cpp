#include <Storages/TimeSeries/PrometheusQueryToSQL/applyClampFunction.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    enum class ClampKind
    {
        Min,
        Max,
        Both,
    };

    struct ImplInfo
    {
        ClampKind kind;
        size_t expected_arguments;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"clamp_min", {ClampKind::Min, 2}},
            {"clamp_max", {ClampKind::Max, 2}},
            {"clamp", {ClampKind::Both, 3}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    void checkArgumentTypes(
        const PQT::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context,
        const ImplInfo & impl_info)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != impl_info.expected_arguments)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, impl_info.expected_arguments, arguments.size());
        }

        if (arguments[0].type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(arguments[0], context), arguments[0].type);
        }

        for (size_t i = 1; i != arguments.size(); ++i)
        {
            if (arguments[i].type != ResultType::SCALAR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument {} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::SCALAR,
                                getPromQLText(arguments[i], context), arguments[i].type);
            }

            if (arguments[i].store_method == StoreMethod::SCALAR_GRID)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Function '{}' with a non-constant scalar parameter is not supported",
                                function_name);
            }
        }
    }

    bool constantClampHasEmptyResult(const std::vector<SQLQueryPiece> & arguments, ClampKind kind)
    {
        /// PromQL clamp(v, min, max) returns an empty vector when max is less than min.
        /// Handle only constant bounds here; non-constant scalar grids are rejected above.
        if (kind != ClampKind::Both)
            return false;

        const auto & min_arg = arguments[1];
        const auto & max_arg = arguments[2];
        return min_arg.store_method == StoreMethod::CONST_SCALAR
            && max_arg.store_method == StoreMethod::CONST_SCALAR
            && max_arg.scalar_value < min_arg.scalar_value;
    }
}


bool isClampFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyClampFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context, *impl_info);

    if (constantClampHasEmptyResult(arguments, impl_info->kind))
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        if (impl_info->kind == ClampKind::Min)
        {
            chassert(args.size() == 2);
            return makeASTFunction(
                "if",
                makeASTFunction("less", args[0]->clone(), args[1]->clone()),
                std::move(args[1]),
                std::move(args[0]));
        }

        if (impl_info->kind == ClampKind::Max)
        {
            chassert(args.size() == 2);
            return makeASTFunction(
                "if",
                makeASTFunction("greater", args[0]->clone(), args[1]->clone()),
                std::move(args[1]),
                std::move(args[0]));
        }

        chassert(args.size() == 3);
        return makeASTFunction(
            "if",
            makeASTFunction("less", args[0]->clone(), args[1]->clone()),
            args[1]->clone(),
            makeASTFunction(
                "if",
                makeASTFunction("greater", args[0]->clone(), args[2]->clone()),
                std::move(args[2]),
                std::move(args[0])));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
