#include <Storages/TimeSeries/PrometheusQueryToSQL/applyNativeHistogramFunction.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a native-histogram function.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 1 argument, but was called with {} arguments",
                            function_name, arguments.size());
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

    /// Returns the name of the ClickHouse scalar function implementing the specified prometheus
    /// native-histogram function. Returns nullptr if not found.
    const char * getCHFunctionName(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, const char *> impl_map = {
            {"histogram_avg", "timeSeriesHistogramAvg"},
            {"histogram_count", "timeSeriesHistogramCount"},
            {"histogram_stddev", "timeSeriesHistogramStddev"},
            {"histogram_stdvar", "timeSeriesHistogramStdvar"},
            {"histogram_sum", "timeSeriesHistogramSum"},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return it->second;
    }
}


bool isNativeHistogramFunction(std::string_view function_name)
{
    return getCHFunctionName(function_name) != nullptr;
}

SQLQueryPiece applyNativeHistogramFunction(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    auto & argument = arguments[0];

    if (argument.store_method != StoreMethod::HISTOGRAM_GRID)
    {
        /// Prometheus silently skips float samples in native-histogram functions, so an argument
        /// without histogram samples produces an empty vector.
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};
    }

    const auto * ch_function_name = getCHFunctionName(function_node->function_name);
    chassert(ch_function_name);

    SQLQueryPiece res{function_node, function_node->result_type, StoreMethod::VECTOR_GRID};
    res.start_time = argument.start_time;
    res.end_time = argument.end_time;
    res.step = argument.step;
    res.metric_name_dropped = argument.metric_name_dropped;

    /// SELECT group, arrayMap((x, k) -> if(equals(k, 1), <ch_function>(x), NULL), histogram_values, sample_kinds) AS values
    /// Steps resolving to a float stay NULL: Prometheus applies these functions to the resolved instant vector and skips float samples.
    SelectQueryBuilder builder;

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    builder.select_list.push_back(makeASTFunction(
        "arrayMap",
        makeASTLambda({"x", "k"}, makeASTFunction(
            "if",
            makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), make_intrusive<ASTLiteral>(UInt64{1})),
            makeASTFunction(ch_function_name, make_intrusive<ASTIdentifier>("x")),
            make_intrusive<ASTLiteral>(Field{}))),
        make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues),
        make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
    builder.from_table = context.subqueries.back().name;

    /// Prometheus omits float-only series from the result; without this filter an all-NULL row would still be
    /// counted by the duplicate-series check in `dropMetricName`. `has` skips NULLs, keeping groups iff some step resolves to a histogram.
    builder.where = makeASTFunction("has",
        make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds),
        make_intrusive<ASTLiteral>(UInt64{1}));

    res.select_query = builder.getSelectQuery();

    /// Function output has no metric name (PromQL semantics).
    return dropMetricName(std::move(res), context);
}

}
