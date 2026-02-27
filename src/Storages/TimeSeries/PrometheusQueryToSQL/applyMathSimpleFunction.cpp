#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathSimpleFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <boost/math/special_functions/sign.hpp>
#include <numbers>


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

    using EvaluateWithConstArgumentFunc = Float64 (*)(Float64);

    struct ImplInfo
    {
        std::string_view ch_function_name;
        EvaluateWithConstArgumentFunc evaluate_with_const_argument;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"abs",
             {
                 "abs",
                 [](Float64 x) -> Float64 { return fabs(x); },
             }},

            {"sgn",
             {
                 "sign",
                 [](Float64 x) -> Float64 { return boost::math::sign(x); },
             }},

            {"floor",
             {
                 "floor",
                 [](Float64 x) -> Float64 { return floor(x); },
             }},

            {"ceil",
             {
                 "ceil",
                 [](Float64 x) -> Float64 { return ceil(x); },
             }},

            {"sqrt",
             {
                 "sqrt",
                 [](Float64 x) -> Float64 { return sqrt(x); },
             }},

            {"exp",
             {
                 "exp",
                 [](Float64 x) -> Float64 { return exp(x); },
             }},

            {"ln",
             {
                 "log",
                 [](Float64 x) -> Float64 { return log(x); },
             }},

            {"log2",
             {
                 "log2",
                 [](Float64 x) -> Float64 { return log2(x); },
             }},

            {"log10",
             {
                 "log10",
                 [](Float64 x) -> Float64 { return log10(x); },
             }},

            {"rad",
             {
                 "radians",
                 [](Float64 x) -> Float64 { return x * (std::numbers::pi / 180.0); },
             }},

            {"deg",
             {
                 "degrees",
                 [](Float64 x) -> Float64 { return x * (180.0 / std::numbers::pi); },
             }},

            {"sin",
             {
                 "sin",
                 [](Float64 x) -> Float64 { return sin(x); },
             }},

            {"cos",
             {
                 "cos",
                 [](Float64 x) -> Float64 { return cos(x); },
             }},

            {"tan",
             {
                 "tan",
                 [](Float64 x) -> Float64 { return tan(x); },
             }},

            {"asin",
             {
                 "asin",
                 [](Float64 x) -> Float64 { return asin(x); },
             }},

            {"acos",
             {
                 "acos",
                 [](Float64 x) -> Float64 { return acos(x); },
             }},

            {"atan",
             {
                 "atan",
                 [](Float64 x) -> Float64 { return atan(x); },
             }},

            {"sinh",
             {
                 "sinh",
                 [](Float64 x) -> Float64 { return sinh(x); },
             }},

            {"cosh",
             {
                 "cosh",
                 [](Float64 x) -> Float64 { return cosh(x); },
             }},

            {"tanh",
             {
                 "tanh",
                 [](Float64 x) -> Float64 { return tanh(x); },
             }},

            {"asinh",
             {
                 "asinh",
                 [](Float64 x) -> Float64 { return asinh(x); },
             }},

            {"acosh",
             {
                 "acosh",
                 [](Float64 x) -> Float64 { return acosh(x); },
             }},

            {"atanh",
             {
                 "atanh",
                 [](Float64 x) -> Float64 { return atanh(x); },
             }},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isMathSimpleFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyMathSimpleFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context);
    auto & argument = arguments[0];

    auto res = argument;
    res.node = function_node;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        {
            res.scalar_value = (impl_info->evaluate_with_const_argument)(argument.scalar_value);
            return res;
        }

        case StoreMethod::SINGLE_SCALAR:
        {
            /// SELECT f(value) AS value FROM <subquery>
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction(impl_info->ch_function_name, make_intrusive<ASTIdentifier>(ColumnNames::Value)));
            builder.select_list.back()->setAlias(ColumnNames::Value);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            res.select_query = builder.getSelectQuery();
            return res;
        }

        case StoreMethod::SCALAR_GRID:
        case StoreMethod::VECTOR_GRID:
        {
            /// For scalar grid:
            /// SELECT arrayMap(x -> f(x), values) AS values
            /// FROM <scalar_grid>
            ///
            /// For vector grid:
            /// SELECT group, arrayMap(x -> f(x), values) AS values
            /// FROM <vector_grid>
            SelectQueryBuilder builder;
            if (argument.store_method == StoreMethod::VECTOR_GRID)
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction(impl_info->ch_function_name, make_intrusive<ASTIdentifier>("x"))),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            res.select_query = builder.getSelectQuery();

            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here because these store methods are incompatible with the allowed argument types
            /// (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    UNREACHABLE();
}

}
