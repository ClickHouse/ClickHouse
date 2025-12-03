#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/addParametersToAggregateFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(const String & promql_function_name, const SQLQueryPiece & argument, const ConverterContext & context)
    {
        if (argument.type != ResultType::RANGE_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function {} expects an argument of type {}, but expression {} has type {}",
                            promql_function_name, ResultType::RANGE_VECTOR,
                            getPromQLQuery(argument, context), argument.type);
        }
    }


    String getSQLFunctionName(const String & promql_function_name)
    {
        static const std::unordered_map<String, String> sql_function_names{
            {"rate", "timeSeriesRateToGrid"},
            {"irate", "timeSeriesInstantRateToGrid"},
            {"delta", "timeSeriesDeltaToGrid"},
            {"idelta", "timeSeriesInstantDeltaToGrid"},
            {"last_over_time", "timeSeriesLastToGrid"},
        };
        auto it = sql_function_names.find(promql_function_name);
        if (it == sql_function_names.end())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", promql_function_name);
        return it->second;
    }


    bool shouldDropMetricName(const String & promql_function_name)
    {
        return promql_function_name != "last_over_time";
    }
}


bool isFunctionOverRange(const String & promql_function_name)
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
    return all_functions_over_range.contains(promql_function_name);
}


SQLQueryPiece applyFunctionOverRange(
    const String & promql_function_name,
    SQLQueryPiece && argument,
    const Node * node,
    ConverterContext & context)
{
    checkArgumentTypes(promql_function_name, argument, context);

    auto evaluation_range = context.node_evaluation_range_getter.get(node);
    auto start_time = evaluation_range.start_time;
    auto end_time = evaluation_range.end_time;
    auto step = evaluation_range.step;
    auto window = evaluation_range.window;

    if (start_time > end_time)
    {
        /// Evaluation range is empty.
        return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
    }

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
            SelectQueryParams params;

            auto aggregate_function = addParametersToAggregateFunction(
                makeASTFunction(
                    getSQLFunctionName(promql_function_name),
                    makeASTFunction(
                        "timeSeriesRange",
                        timeseriesTimeToAST(argument.start_time),
                        timeseriesTimeToAST(argument.end_time),
                        timeseriesDurationToAST(argument.step)),
                    makeASTFunction(
                        "arrayResize",
                        std::make_shared<ASTLiteral>(Array{}),
                        std::make_shared<ASTLiteral>(countTimeseriesSteps(argument.start_time, argument.end_time, argument.step)),
                        std::make_shared<ASTLiteral>(argument.scalar_value))),
                start_time,
                end_time,
                step,
                window);

            params.select_list.push_back(aggregate_function);
            params.select_list.back()->setAlias(ColumnNames::Values);

            res.select_query = buildSelectQuery(std::move(params));
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
            SelectQueryParams params;

            if (argument.store_method == StoreMethod::VECTOR_GRID)
                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            ASTPtr timestamps_argument, values_argument;
            if (argument.store_method == StoreMethod::SCALAR_GRID)
            {
                timestamps_argument = makeASTFunction(
                    "timeSeriesRange",
                    timeseriesTimeToAST(argument.start_time),
                    timeseriesTimeToAST(argument.end_time),
                    timeseriesDurationToAST(argument.step));
                values_argument = std::make_shared<ASTIdentifier>(ColumnNames::Values);
            }
            else
            {
                ASTPtr ts = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(argument.start_time),
                    timeseriesTimeToAST(argument.end_time),
                    timeseriesDurationToAST(argument.step),
                    std::make_shared<ASTIdentifier>(ColumnNames::Values));
                ts->setAlias(ColumnNames::TimeSeries);
                timestamps_argument = makeASTFunction("tupleElement", std::move(ts), std::make_shared<ASTLiteral>(1));
                values_argument = makeASTFunction(
                    "tupleElement", std::make_shared<ASTIdentifier>(ColumnNames::TimeSeries), std::make_shared<ASTLiteral>(2));
            }

            auto aggregate_function = addParametersToAggregateFunction(
                makeASTFunction(getSQLFunctionName(promql_function_name), timestamps_argument, values_argument),
                start_time,
                end_time,
                step,
                window);

            params.select_list.push_back(aggregate_function);
            params.select_list.back()->setAlias(ColumnNames::Values);

            if (argument.store_method == StoreMethod::VECTOR_GRID)
                params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            params.from_table = subqueries.back().name;

            res.select_query = buildSelectQuery(std::move(params));
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            if ((res.store_method == StoreMethod::VECTOR_GRID) && shouldDropMetricName(promql_function_name))
                res = dropMetricName(std::move(res), context);

            return res;
        }

        case StoreMethod::RAW_DATA:
        {
            /// SELECT group,
            ///        <aggregate_function>(timestamps, values) AS values
            /// FROM <raw_data>
            /// GROUP BY group
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            auto aggregate_function = addParametersToAggregateFunction(
                makeASTFunction(
                    getSQLFunctionName(promql_function_name),
                    std::make_shared<ASTIdentifier>(ColumnNames::Timestamp),
                    std::make_shared<ASTIdentifier>(ColumnNames::Value)),
                start_time,
                end_time,
                step,
                window);

            params.select_list.push_back(aggregate_function);
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
            params.from_table = subqueries.back().name;

            res.select_query = buildSelectQuery(std::move(params));

            res.store_method = StoreMethod::VECTOR_GRID;
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            if (shouldDropMetricName(promql_function_name))
                res = dropMetricName(std::move(res), context);

            return res;
        }

        case StoreMethod::CONST_STRING:
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Argument for function '{}' in expression {} has unexpected type {} (store_method: {})",
                            promql_function_name, getPromQLQuery(argument, context), argument.type, argument.store_method);
        }
    }

    UNREACHABLE();
}

}
