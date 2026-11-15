#include <Storages/TimeSeries/PrometheusQueryToSQL/applyClampFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>

#include <cmath>
#include <limits>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a clamping function.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        size_t expected_num_arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != expected_num_arguments)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, expected_num_arguments, arguments.size());
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
                                "Function '{}' expects argument #{} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::SCALAR,
                                getPromQLText(arguments[i], context), arguments[i].type);
            }
        }
    }
}


bool isClampFunction(std::string_view function_name)
{
    return (function_name == "clamp") || (function_name == "clamp_min") || (function_name == "clamp_max");
}


SQLQueryPiece applyClampFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;

    bool has_min = (function_name != "clamp_max");
    bool has_max = (function_name != "clamp_min");
    size_t num_arguments = 1 + has_min + has_max;

    checkArgumentTypes(function_node, arguments, num_arguments, context);

    size_t min_index = 1;
    size_t max_index = has_min ? 2 : 1;

    bool min_is_const = has_min && (arguments[min_index].store_method == StoreMethod::CONST_SCALAR);
    bool max_is_const = has_max && (arguments[max_index].store_method == StoreMethod::CONST_SCALAR);

    /// PromQL: clamp() returns an empty vector if max < min. If both bounds are constant we know that already,
    /// otherwise the same check is added to the SQL expression below.
    if (min_is_const && max_is_const && (arguments[max_index].scalar_value < arguments[min_index].scalar_value))
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    /// PromQL: the result is NaN if any of the bounds is NaN.
    bool const_nan_bound = (min_is_const && std::isnan(arguments[min_index].scalar_value))
        || (max_is_const && std::isnan(arguments[max_index].scalar_value));

    /// If the bounds aren't constant then the check for `max < min` is done in the SQL expression (see below).
    bool check_max_less_min_in_sql = has_min && has_max && !(min_is_const && max_is_const) && !const_nan_bound;

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        chassert(args.size() == num_arguments);
        const ASTPtr & x = args[0];

        if (const_nan_bound)
        {
            /// One of the bounds is a NaN constant, so every value becomes NaN
            /// (and `x + nan` keeps NULLs, i.e. time steps without a value, as is).
            return makeASTFunction("plus", x->clone(), make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::quiet_NaN()));
        }

        ASTPtr min_ast = has_min ? args[min_index] : nullptr;
        ASTPtr max_ast = has_max ? args[max_index] : nullptr;

        /// greatest(min, least(max, x))
        ASTPtr clamped = x->clone();
        if (max_ast)
            clamped = makeASTFunction("least", max_ast->clone(), std::move(clamped));
        if (min_ast)
            clamped = makeASTFunction("greatest", min_ast->clone(), std::move(clamped));

        /// If a bound isn't constant then whether it's NaN is checked in the SQL expression:
        /// if(isNaN(min) OR isNaN(max), nan, <clamped>)
        ASTPtr bounds_nan_condition;
        if (has_min && !min_is_const)
            bounds_nan_condition = makeASTFunction("isNaN", min_ast->clone());
        if (has_max && !max_is_const)
        {
            ASTPtr is_nan = makeASTFunction("isNaN", max_ast->clone());
            bounds_nan_condition = bounds_nan_condition
                ? makeASTFunction("or", std::move(bounds_nan_condition), std::move(is_nan))
                : std::move(is_nan);
        }
        if (bounds_nan_condition)
        {
            clamped = makeASTFunction("if",
                std::move(bounds_nan_condition),
                make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::quiet_NaN()),
                std::move(clamped));
        }

        /// PromQL keeps NaN values as is: clamp(NaN, min, max) == NaN, and least()/greatest() don't guarantee that.
        /// NULLs (i.e. time steps without a value) must be kept as is too:
        /// if(isNull(x) OR isNaN(x), x, <clamped>)
        ASTPtr res = makeASTFunction("if",
            makeASTFunction("or",
                makeASTFunction("isNull", x->clone()),
                makeASTFunction("isNaN", x->clone())),
            x->clone(),
            std::move(clamped));

        /// If the bounds aren't constant then the check for `max < min` is done in the SQL expression:
        /// if(max < min, NULL, <res>)
        if (check_max_less_min_in_sql)
        {
            res = makeASTFunction("if",
                makeASTFunction("less", max_ast->clone(), min_ast->clone()),
                make_intrusive<ASTLiteral>(Field{} /* NULL */),
                std::move(res));
        }

        return res;
    };

    /// Since the `max < min` check in the SQL expression may convert values to NULL we always need VECTOR_GRID
    /// to represent the result. So we cast the vector argument to VECTOR_GRID to enforce that.
    if (check_max_less_min_in_sql)
        arguments[0] = toVectorGrid(std::move(arguments[0]), context);

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
