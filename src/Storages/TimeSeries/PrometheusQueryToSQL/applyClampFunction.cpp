#include <Storages/TimeSeries/PrometheusQueryToSQL/applyClampFunction.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
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
        }
    }

    bool clampMayProduceEmptyVector(const std::vector<SQLQueryPiece> & arguments, ClampKind kind)
    {
        if (kind != ClampKind::Both)
            return false;

        const auto & min_arg = arguments[1];
        const auto & max_arg = arguments[2];
        if (min_arg.store_method != StoreMethod::CONST_SCALAR || max_arg.store_method != StoreMethod::CONST_SCALAR)
            return true;

        return max_arg.scalar_value < min_arg.scalar_value;
    }

    ASTPtr keepNullSamplesSparse(ASTPtr value, ASTPtr clamped_value)
    {
        return makeASTFunction(
            "if",
            makeASTFunction("isNull", value->clone()),
            make_intrusive<ASTLiteral>(Field{}),
            std::move(clamped_value));
    }

    ASTPtr makeClampMin(ASTPtr value, ASTPtr min_bound)
    {
        ASTPtr clamped_value = makeASTFunction(
            "if",
            makeASTFunction("isNaN", min_bound->clone()),
            min_bound->clone(),
            makeASTFunction(
                "if",
                makeASTFunction("less", value->clone(), min_bound->clone()),
                min_bound->clone(),
                value->clone()));
        return keepNullSamplesSparse(std::move(value), std::move(clamped_value));
    }

    ASTPtr makeClampMax(ASTPtr value, ASTPtr max_bound)
    {
        ASTPtr clamped_value = makeASTFunction(
            "if",
            makeASTFunction("isNaN", max_bound->clone()),
            max_bound->clone(),
            makeASTFunction(
                "if",
                makeASTFunction("greater", value->clone(), max_bound->clone()),
                max_bound->clone(),
                value->clone()));
        return keepNullSamplesSparse(std::move(value), std::move(clamped_value));
    }

    ASTPtr makeClampBoth(ASTPtr value, ASTPtr min_bound, ASTPtr max_bound)
    {
        auto min_is_nan = makeASTFunction("isNaN", min_bound->clone());
        auto max_is_nan = makeASTFunction("isNaN", max_bound->clone());
        auto bound_is_nan = makeASTFunction("or", min_is_nan->clone(), max_is_nan->clone());
        auto bounds_are_empty = makeASTFunction(
            "and",
            makeASTFunction("not", std::move(min_is_nan)),
            makeASTFunction("not", std::move(max_is_nan)),
            makeASTFunction("less", max_bound->clone(), min_bound->clone()));

        ASTPtr clamped_value = makeASTFunction(
            "if",
            std::move(bounds_are_empty),
            make_intrusive<ASTLiteral>(Field{}),
            makeASTFunction(
                "if",
                std::move(bound_is_nan),
                makeASTFunction("plus", min_bound->clone(), max_bound->clone()),
                makeASTFunction(
                    "if",
                    makeASTFunction("less", value->clone(), min_bound->clone()),
                    min_bound->clone(),
                    makeASTFunction(
                        "if",
                        makeASTFunction("greater", value->clone(), max_bound->clone()),
                        max_bound->clone(),
                        value->clone()))));
        return keepNullSamplesSparse(std::move(value), std::move(clamped_value));
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

    if (clampMayProduceEmptyVector(arguments, impl_info->kind))
        arguments[0] = toVectorGrid(std::move(arguments[0]), context);

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        if (impl_info->kind == ClampKind::Min)
        {
            chassert(args.size() == 2);
            return makeClampMin(std::move(args[0]), std::move(args[1]));
        }

        if (impl_info->kind == ClampKind::Max)
        {
            chassert(args.size() == 2);
            return makeClampMax(std::move(args[0]), std::move(args[1]));
        }

        chassert(args.size() == 3);
        return makeClampBoth(std::move(args[0]), std::move(args[1]), std::move(args[2]));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
