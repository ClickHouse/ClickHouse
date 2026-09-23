#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>

#include <Core/DecimalFunctions.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <base/arithmeticOverflow.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Applies an offset for the evaluation time: <expression> offset 1d
    SQLQueryPiece offsetEvaluationTime(
        const PrometheusQueryTree::Offset * offset_node,
        SQLQueryPiece && expression,
        DurationType offset_value,
        ConverterContext & context)
    {
        expression.node = offset_node;

        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::SINGLE_SCALAR:
            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                expression.start_time += offset_value;
                expression.end_time += offset_value;
                return std::move(expression);
            }

            case StoreMethod::RAW_DATA:
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                /// The interval functions don't accept Decimal arguments, so we choose the unit (milliseconds, microseconds
                /// or nanoseconds) which is not longer than the tick of `result_timestamp_scale`, and pass an integer number of the units.
                UInt32 result_scale = context.result_timestamp_scale;
                chassert(result_scale <= 9); /// Maximum scale for DateTime64 is 9 (nanoseconds).
                UInt32 interval_scale = (result_scale + 2) / 3 * 3;
                Int64 offset_in_interval_units = 0;
                if (common::mulOverflow(offset_value.value, DecimalUtils::scaleMultiplier<Int64>(interval_scale - result_scale), offset_in_interval_units))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Offset {} is too big in expression {}",
                                    toString(offset_value, result_scale), getPromQLText(expression, context));
                }

                static const std::string_view to_interval_functions[] = {"toIntervalSecond", "toIntervalMillisecond", "toIntervalMicrosecond", "toIntervalNanosecond"};
                std::string_view to_interval_function = to_interval_functions[interval_scale / 3];

                /// The column `timestamp` is converted to `result_timestamp_type` before adding the interval because it can have
                /// a type which doesn't support intervals (UInt32). Adding an interval can change the scale
                /// (for example, DateTime64(4) + INTERVAL 1 MICROSECOND is DateTime64(6)), so we cast the sum back.
                /// Both casts do nothing if the types already match.
                const String result_timestamp_type_name = context.result_timestamp_type->getName();
                auto add_offset = [&](ASTPtr timestamp)
                {
                    return makeASTFunction(
                        "CAST",
                        makeASTFunction(
                            "plus",
                            makeASTFunction(
                                "CAST", std::move(timestamp), make_intrusive<ASTLiteral>(result_timestamp_type_name)),
                            makeASTFunction(to_interval_function, make_intrusive<ASTLiteral>(offset_in_interval_units))),
                        make_intrusive<ASTLiteral>(result_timestamp_type_name));
                };

                if (context.time_series_version >= TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES)
                {
                    /// SELECT group, arrayMap(sample -> (sample.1 + INTERVAL X, sample.2), time_series) AS time_series
                    auto new_sample = makeASTFunction(
                        "tuple",
                        add_offset(makeASTFunction(
                            "tupleElement", make_intrusive<ASTIdentifier>("sample"), make_intrusive<ASTLiteral>(UInt64{1}))),
                        makeASTFunction(
                            "tupleElement", make_intrusive<ASTIdentifier>("sample"), make_intrusive<ASTLiteral>(UInt64{2})));

                    auto new_time_series = makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>("sample")), std::move(new_sample)),
                        make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries));
                    new_time_series->setAlias(ColumnNames::TimeSeries);
                    builder.select_list.push_back(std::move(new_time_series));
                }
                else
                {
                    /// SELECT group, CAST(CAST(timestamp, 'result_timestamp_type') + INTERVAL <x> <unit>, 'result_timestamp_type') AS timestamp, value
                    auto new_timestamp = add_offset(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
                    new_timestamp->setAlias(ColumnNames::Timestamp);
                    builder.select_list.push_back(std::move(new_timestamp));
                    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));
                }

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
                builder.from_table = subqueries.back().name;

                expression.select_query = builder.getSelectQuery();

                return std::move(expression);
            }
        }

        UNREACHABLE();
    }

    /// Applies a fixed evaluation time: <expression> @ 1609746000, @ start(), or @ end()
    SQLQueryPiece setEvaluationTime(
        const PrometheusQueryTree::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context)
    {
        /// A range vector already contains the samples evaluated at the fixed time. Keep its timestamps and, for a
        /// subquery, its complete inner grid intact. A range-vector function will aggregate it at the fixed time.
        if (expression.type == ResultType::RANGE_VECTOR)
        {
            expression.node = offset_node;
            return std::move(expression);
        }

        auto node_range = context.node_range_getter.get(offset_node);
        if (node_range.empty())
            return SQLQueryPiece{offset_node, offset_node->result_type, StoreMethod::EMPTY};

        /// <expression> is expected to be calculated at a fixed evaluation time.
        if (expression.start_time != expression.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to be calculated at a fixed evaluation time",
                            getPromQLText(expression, context));
        }

        expression.node = offset_node;

        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::SINGLE_SCALAR:
            {
                expression.start_time = node_range.start_time;
                expression.end_time = node_range.end_time;
                expression.step = node_range.step;
                return std::move(expression);
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// For scalar grid:
                /// SELECT arrayResize([], <count_of_time_steps>, values[1])) AS values
                /// FROM <scalar_grid>
                ///
                /// For vector grid:
                /// SELECT group,
                ///        arrayResize([], <count_of_time_steps>, values[1])) AS values
                /// FROM <vector_grid>
                SelectQueryBuilder builder;

                if (expression.store_method == StoreMethod::VECTOR_GRID)
                    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                auto new_values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(
                        stepsInTimeSeriesRange(node_range.start_time, node_range.end_time, node_range.step)),
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));

                new_values->setAlias(ColumnNames::Values);
                builder.select_list.push_back(std::move(new_values));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
                builder.from_table = subqueries.back().name;

                expression.select_query = builder.getSelectQuery();

                expression.start_time = node_range.start_time;
                expression.end_time = node_range.end_time;
                expression.step = node_range.step;
                return std::move(expression);
            }

            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because RAW_DATA is used only for range vectors, and they are returned above as is.
                throwUnexpectedStoreMethod(expression, context);
            }
        }

        UNREACHABLE();
    }
}

SQLQueryPiece applyOffset(const PrometheusQueryTree::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context)
{
    if (offset_node->hasAtModifier())
    {
        /// Set fixed evaluation time.
        return setEvaluationTime(offset_node, std::move(expression), context);
    }
    else if (auto offset_value = offset_node->offset_value)
    {
        /// Add offset to the evaluation time.
        return offsetEvaluationTime(offset_node, std::move(expression), *offset_value, context);
    }
    else
    {
        return expression;
    }
}

}
