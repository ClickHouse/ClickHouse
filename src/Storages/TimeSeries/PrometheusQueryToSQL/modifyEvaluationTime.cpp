#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyEvaluationTime.h>

#include <Core/DecimalFunctions.h>
#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTime.h>


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
        const PQT::At * at_node,
        SQLQueryPiece && expression,
        const DecimalField<Decimal64> & offset,
        ConverterContext & context)
    {
        expression.node = at_node;

        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                expression.start_time = addTimeseriesDuration(expression.start_time, offset);
                expression.end_time = addTimeseriesDuration(expression.end_time, offset);
                return std::move(expression);
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT group, timestamp + INTERVAL X, value
                /// FROM <raw_data>
                SelectQueryParams params;

                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

                /// Round up the scale to next number divisible by 3 but not greater than 9 (nanoseconds scale).
                UInt32 scale = std::max<UInt32>((context.time_scale + 2) / 3 * 3, 9);
                Int64 scaled_offset = DecimalUtils::convertTo<Decimal64>(scale, offset.getValue(), offset.getScale());

                static const std::string_view interval_functions[] = {"toIntervalSecond", "toIntervalMillisecond", "toIntervalMicrosecond", "toIntervalNanosecond"};
                std::string_view interval_function = interval_functions[scale / 3];

                ASTPtr new_timestamp = makeASTFunction(
                    "plus",
                    std::make_shared<ASTIdentifier>(ColumnNames::Timestamp),
                    makeASTFunction(interval_function, std::make_shared<ASTLiteral>(scaled_offset)));

                params.select_list.push_back(new_timestamp);
                params.select_list.back()->setAlias(ColumnNames::Timestamp);

                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Value));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
                params.from_table = subqueries.back().name;

                expression.select_query = buildSelectQuery(std::move(params));

                return std::move(expression);
            }
        }

        UNREACHABLE();
    }

    /// Applies setting a fixed evaluation time: <expression> @ 1609746000
    SQLQueryPiece setEvaluationTime(const PQT::At * at_node, SQLQueryPiece && expression, ConverterContext & context)
    {
        /// <expression> is expected to be calculated at a fixed evaluation time.
        if (expression.start_time != expression.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to be calculated at a fixed evaluation time",
                            getPromQLQuery(expression, context));
        }

        auto evaluation_range = context.node_evaluation_range_getter.get(at_node);

        if (evaluation_range.start_time > evaluation_range.end_time)
            return SQLQueryPiece{at_node, at_node->result_type, StoreMethod::EMPTY};

        expression.node = at_node;

        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            {
                expression.start_time = evaluation_range.start_time;
                expression.end_time = evaluation_range.end_time;
                expression.step = evaluation_range.step;
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
                SelectQueryParams params;

                if (expression.store_method == StoreMethod::VECTOR_GRID)
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

                params.select_list.push_back(makeASTFunction(
                    "arrayResize",
                    std::make_shared<ASTLiteral>(Array{}),
                    std::make_shared<ASTLiteral>(
                        countTimeseriesSteps(evaluation_range.start_time, evaluation_range.end_time, evaluation_range.step)),
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(ColumnNames::Values), std::make_shared<ASTLiteral>(1u))));

                params.select_list.back()->setAlias(ColumnNames::Values);

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
                params.from_table = subqueries.back().name;

                expression.select_query = buildSelectQuery(std::move(params));

                expression.start_time = evaluation_range.start_time;
                expression.end_time = evaluation_range.end_time;
                expression.step = evaluation_range.step;
                return std::move(expression);
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT group,
                ///        arrayJoin(timeSeriesRange(<start_time>, <end_time>, <step>)) AS timestamp,
                ///        value
                /// FROM <raw_data>
                SelectQueryParams params;

                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

                params.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "timeSeriesRange",
                        timeseriesTimeToAST(evaluation_range.start_time),
                        timeseriesTimeToAST(evaluation_range.end_time),
                        timeseriesDurationToAST(evaluation_range.step))));

                params.select_list.back()->setAlias(ColumnNames::Timestamp);

                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Value));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
                params.from_table = subqueries.back().name;

                expression.select_query = buildSelectQuery(std::move(params));

                return std::move(expression);
            }
        }

        UNREACHABLE();
    }
}

SQLQueryPiece modifyEvaluationTime(const PQT::At * at_node, SQLQueryPiece && expression, ConverterContext & context)
{
    if (at_node->getAt())
    {
        /// Set fixed evaluation time.
        return setEvaluationTime(at_node, std::move(expression), context);
    }
    else if (const auto * offset = at_node->getOffset())
    {
        /// Add offset to the evaluation time.
        auto offset_value = nodeToDuration(offset, context.time_scale);
        return offsetEvaluationTime(at_node, std::move(expression), offset_value, context);
    }
    else
    {
        return expression;
    }
}

}
