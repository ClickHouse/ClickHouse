#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>

#include <Core/DecimalFunctions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Applies an offset for the evaluation time: <expression> offset 1d
    SQLQueryPiece offsetEvaluationTime(
        const PQT::Offset * offset_node,
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
                /// SELECT group, timestamp + INTERVAL X, value
                /// FROM <raw_data>
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                ASTPtr new_timestamp;
                if (isDateTime64(context.timestamp_data_type))
                {
                    /// timestamp + INTERVAL x MILLISECONDS
                    chassert(context.timestamp_scale <= 9); /// Maximum scale for DateTime64 is 9 (nanoseconds).
                    /// Round up the scale to next number divisible by 3.
                    UInt32 scale = std::max<UInt32>((context.timestamp_scale + 2) / 3 * 3, 9);
                    Decimal64 scaled_offset_value = DecimalUtils::convertTo<Decimal64>(scale, offset_value, context.timestamp_scale);

                    static const std::string_view to_interval_functions[] = {"toIntervalSecond", "toIntervalMillisecond", "toIntervalMicrosecond", "toIntervalNanosecond"};
                    std::string_view to_interval_function = to_interval_functions[scale / 3];

                    new_timestamp = makeASTFunction(
                        "plus",
                        make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                        makeASTFunction(to_interval_function, make_intrusive<ASTLiteral>(scaled_offset_value)));
                }
                else
                {
                    /// timestamp + x
                    new_timestamp = makeASTFunction(
                        "plus",
                        make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                        timeSeriesDurationToAST(offset_value, context.timestamp_data_type));
                }

                new_timestamp->setAlias(ColumnNames::Timestamp);
                builder.select_list.push_back(std::move(new_timestamp));

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
                builder.from_table = subqueries.back().name;

                expression.select_query = builder.getSelectQuery();

                return std::move(expression);
            }
        }

        UNREACHABLE();
    }

    /// Applies setting a fixed evaluation time: <expression> @ 1609746000
    SQLQueryPiece setEvaluationTime(const PQT::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context)
    {
        /// <expression> is expected to be calculated at a fixed evaluation time.
        if (expression.start_time != expression.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to be calculated at a fixed evaluation time",
                            getPromQLText(expression, context));
        }

        auto node_range = context.node_range_getter.get(offset_node);
        if (node_range.empty())
            return SQLQueryPiece{offset_node, offset_node->result_type, StoreMethod::EMPTY};

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
                /// SELECT group,
                ///        arrayJoin(timeSeriesRange(<start_time>, <end_time>, <step>)) AS timestamp,
                ///        value
                /// FROM <raw_data>
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                auto new_timestamp = makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "timeSeriesRange",
                        timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
                        timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
                        timeSeriesDurationToAST(node_range.step, context.timestamp_data_type)));

                new_timestamp->setAlias(ColumnNames::Timestamp);
                builder.select_list.push_back(std::move(new_timestamp));

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
                builder.from_table = subqueries.back().name;

                expression.select_query = builder.getSelectQuery();

                return std::move(expression);
            }
        }

        UNREACHABLE();
    }
}

SQLQueryPiece applyOffset(const PQT::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context)
{
    if (offset_node->at_timestamp)
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
