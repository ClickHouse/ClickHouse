#include <Storages/TimeSeries/PrometheusQueryToSQL/applyHistogramFraction.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the `histogram_fraction` function.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 3)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 3 arguments, but was called with {} arguments",
                            function_name, arguments.size());
        }

        for (size_t i = 0; i < 2; ++i)
        {
            if (arguments[i].type != ResultType::SCALAR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument {} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::SCALAR,
                                getPromQLText(arguments[i], context), arguments[i].type);
            }
        }

        if (arguments[2].type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects third argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(arguments[2], context), arguments[2].type);
        }
    }
}


bool isHistogramFraction(std::string_view function_name)
{
    return function_name == "histogram_fraction";
}

SQLQueryPiece applyHistogramFraction(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    const auto & lower_arg = arguments[0];
    const auto & upper_arg = arguments[1];
    auto & expression = arguments[2];

    if (lower_arg.store_method != StoreMethod::CONST_SCALAR)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Function 'histogram_fraction' currently requires a constant lower parameter");
    }
    if (upper_arg.store_method != StoreMethod::CONST_SCALAR)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Function 'histogram_fraction' currently requires a constant upper parameter");
    }

    if (expression.store_method != StoreMethod::HISTOGRAM_GRID)
    {
        /// Prometheus skips float samples in native-histogram functions, and `histogram_fraction` over classic buckets
        /// is unsupported, so an argument without histogram samples produces an empty vector.
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};
    }

    const Float64 lower = lower_arg.scalar_value;
    const Float64 upper = upper_arg.scalar_value;

    SQLQueryPiece res{function_node, function_node->result_type, StoreMethod::VECTOR_GRID};
    res.start_time = expression.start_time;
    res.end_time = expression.end_time;
    res.step = expression.step;
    res.metric_name_dropped = expression.metric_name_dropped;

    /// Per-step `timeSeriesHistogramFraction` over the grid's histogram samples. Steps whose newest sample is a float stay NULL
    /// (Prometheus skips floats), and groups with no histogram sample in range are dropped.
    SelectQueryBuilder builder;

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    builder.select_list.push_back(makeASTFunction(
        "arrayMap",
        makeASTLambda({"x", "k"}, makeASTFunction(
            "if",
            makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), make_intrusive<ASTLiteral>(UInt64{1})),
            makeASTFunction("timeSeriesHistogramFraction",
                make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(lower), make_intrusive<ASTLiteral>(upper)),
            make_intrusive<ASTLiteral>(Field{}))),
        make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues),
        make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
    builder.from_table = context.subqueries.back().name;

    builder.where = makeASTFunction("has",
        make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds),
        make_intrusive<ASTLiteral>(UInt64{1}));

    res.select_query = builder.getSelectQuery();

    /// Function output has no metric name (PromQL semantics).
    return dropMetricName(std::move(res), context);
}

}
