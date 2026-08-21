#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the function.
    void checkArgumentTypes(const String & function_name, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        size_t expected_number_of_arguments = 1;

        if (arguments.size() != expected_number_of_arguments)
        {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} {}, got {} arguments",
                                function_name, expected_number_of_arguments, (expected_number_of_arguments == 1 ? "argument" : "arguments"),
                                arguments.size());
        }

        const auto & argument = arguments[0];
        if (argument.type != ResultType::RANGE_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function {} expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::RANGE_VECTOR,
                            getPromQLQuery(argument, context), argument.type);
        }
    }

    /// Returns an AST to do the aggregation over time.
    ASTPtr makeExpressionForResultVector(
        const String & function_name,
        const NodeEvaluationRange & node_range,
        ASTPtr && timestamps_argument,
        ASTPtr && values_argument,
        ConverterContext & context)
    {
        static const std::unordered_map<String, String> sql_function_names{
            {"rate", "timeSeriesRateToGrid"},
            {"irate", "timeSeriesInstantRateToGrid"},
            {"delta", "timeSeriesDeltaToGrid"},
            {"idelta", "timeSeriesInstantDeltaToGrid"},
            {"last_over_time", "timeSeriesLastToGrid"},
        };

        auto it = sql_function_names.find(function_name);
        if (it == sql_function_names.end())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function_name);
        const auto & sql_function_name = it->second;

        return addParametersToAggregateFunction(
            makeASTFunction(sql_function_name, std::move(timestamps_argument), std::move(values_argument)),
            timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(node_range.step, context.timestamp_data_type),
            timeSeriesDurationToAST(node_range.window, context.timestamp_data_type));
    }

    bool shouldDropMetricName(const String & promql_function_name)
    {
        return promql_function_name != "last_over_time";
    }
}


bool isFunctionOverRange(const String & function_name)
{
    static const std::unordered_set<std::string_view> all_functions_over_range = {
        "rate",
        "irate",
        "delta",
        "idelta",
        "resets",
        "predict_linear",
        "deriv",
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "sum_over_time",
        "count_over_time",
        "quantile_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "last_over_time",
        "present_over_time",
        "absent_over_time",
        "mad_over_time",
        "ts_of_min_over_time",
        "ts_of_max_over_time",
        "ts_of_last_over_time",
        "first_over_time",
        "ts_of_first_over_time",
    };
    return all_functions_over_range.contains(function_name);
}


SQLQueryPiece applyFunctionOverRange(
    const Node * node,
    const String & function_name,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkArgumentTypes(function_name, arguments, context);

    auto node_range = context.node_range_getter.get(node);
    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;

    if (start_time > end_time)
    {
        /// Evaluation range is empty.
        return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
    }

    auto argument = std::move(arguments[0]);

    SQLQueryPiece res = argument;
    res.node = node;
    res.type = ResultType::INSTANT_VECTOR;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        {
            /// SELECT <aggregate_function>(timeSeriesRange(<start_time>, <end_time>, <step>),
            ///                             arrayResize([], <count_of_time_steps>, <scalar_value>)) AS values
            SelectQueryBuilder builder;

            auto new_values = makeExpressionForResultVector(
                function_name,
                node_range,
                makeASTFunction(
                    "timeSeriesRange",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(argument.step, context.timestamp_data_type)),
                makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(argument.start_time, argument.end_time, argument.step)),
                    timeSeriesScalarToAST(argument.scalar_value, context.scalar_data_type)),
                context);

            new_values->setAlias(ColumnNames::Values);
            builder.select_list.push_back(std::move(new_values));

            res.select_query = builder.getSelectQuery();
            res.scalar_value = {};

            res.store_method = StoreMethod::SCALAR_GRID;
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            return res;
        }

        case StoreMethod::SCALAR_GRID:
        case StoreMethod::VECTOR_GRID:
        {
            /// For scalar grid:
            /// SELECT <aggregate_function>(timeSeriesRange(<start_time>, <end_time>, <step>), values) AS values
            /// FROM <scalar_grid>
            ///
            /// For vector grid:
            /// SELECT group,
            ///        <aggregate_function>((timeSeriesFromGrid(<start_time>, <end_time>, <step>, values) AS time_series).1, time_series.2) AS values
            /// FROM <vector_grid>
            /// GROUP BY group
            SelectQueryBuilder builder;

            if (argument.store_method == StoreMethod::VECTOR_GRID)
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            ASTPtr timestamps_argument;
            ASTPtr values_argument;
            if (argument.store_method == StoreMethod::SCALAR_GRID)
            {
                timestamps_argument = makeASTFunction(
                    "timeSeriesRange",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(argument.step, context.timestamp_data_type));
                values_argument = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            }
            else
            {
                ASTPtr ts = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                    make_intrusive<ASTIdentifier>(ColumnNames::Values));
                ts->setAlias(ColumnNames::TimeSeries);
                timestamps_argument = makeASTFunction("tupleElement", std::move(ts), make_intrusive<ASTLiteral>(1));
                values_argument = makeASTFunction(
                    "tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries), make_intrusive<ASTLiteral>(2));
            }

            auto new_values = makeExpressionForResultVector(
                function_name, node_range, std::move(timestamps_argument), std::move(values_argument), context);

            new_values->setAlias(ColumnNames::Values);
            builder.select_list.push_back(std::move(new_values));

            if (argument.store_method == StoreMethod::VECTOR_GRID)
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
            builder.from_table = subqueries.back().name;

            res.select_query = builder.getSelectQuery();
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            if ((res.store_method == StoreMethod::VECTOR_GRID) && shouldDropMetricName(function_name))
                res = dropMetricName(std::move(res), context);

            return res;
        }

        case StoreMethod::RAW_DATA:
        {
            /// SELECT group,
            ///        <aggregate_function>(timestamps, values) AS values
            /// FROM <raw_data>
            /// GROUP BY group
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            auto new_values = makeExpressionForResultVector(
                function_name,
                node_range,
                make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                make_intrusive<ASTIdentifier>(ColumnNames::Value),
                context);

            new_values->setAlias(ColumnNames::Values);
            builder.select_list.push_back(std::move(new_values));

            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
            builder.from_table = subqueries.back().name;

            res.select_query = builder.getSelectQuery();

            res.store_method = StoreMethod::VECTOR_GRID;
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            if (shouldDropMetricName(function_name))
                res = dropMetricName(std::move(res), context);

            return res;
        }

        case StoreMethod::CONST_STRING:
        {
            /// Can't get in here, the store method CONST_STRING is incompatible
            /// with the allowed argument types (see checkArgumentTypes()).
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Argument for function '{}' in expression {} has unexpected type {} (store_method: {})",
                            function_name, getPromQLQuery(argument, context), argument.type, argument.store_method);
        }
    }

    UNREACHABLE();
}

}
