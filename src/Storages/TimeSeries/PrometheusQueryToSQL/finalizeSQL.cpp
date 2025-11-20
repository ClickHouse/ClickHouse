#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Prepares expressions for ORDER BY in case we sort by specific tags and then by all tags (See ResultSorting::Mode::ORDERED_BY_TAGS).
    ASTs getOrderByTagsExpressions(const Strings & sorting_tags)
    {
        ASTs list;

        /// First we sort by specified tags,
        for (const auto & sorting_tag : sorting_tags)
        {
            list.push_back(makeASTFunction(
                "timeSeriesExtractTag",
                std::make_shared<ASTIdentifier>(ColumnNames::Group),
                std::make_shared<ASTLiteral>(sorting_tag)));
        }

        /// then by all tags.
        list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Tags));

        return list;
    }


    /// Finalizes a SQL query returning a scalar as two columns "time", "value".
    ASTPtr finalizeScalarAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::SCALAR);

        if (result.start_time != result.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to produce a scalar result, got multiple values at different times",
                            getPromQLQuery(result, context));
        }

        switch (result.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT <start_time> AS time, <scalar_value> AS value
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_time_type));
                params.select_list.back()->setAlias(ColumnNames::Time);

                params.select_list.push_back(std::make_shared<ASTLiteral>(result.scalar_value));
                params.select_list.back()->setAlias(ColumnNames::Value);

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT <start_time> AS time, toFloat64(values[1]) AS value
                /// FROM <scalar_grid>
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_time_type));
                params.select_list.back()->setAlias(ColumnNames::Time);

                params.select_list.push_back(makeASTFunction(
                    "toFloat64",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(ColumnNames::Values), std::make_shared<ASTLiteral>(1u))));
                params.select_list.back()->setAlias(ColumnNames::Value);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_table = params.with.back().name;

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::EMPTY:
            case StoreMethod::CONST_STRING:
            case StoreMethod::VECTOR_GRID:
            case StoreMethod::RAW_DATA:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Expression {} is of type {} and can't use store method {}",
                                getPromQLQuery(result, context), result.type, result.store_method);
            }
        }
        UNREACHABLE();
    }


    /// Finalizes a SQL query returning a string as two columns "time", "value".
    ASTPtr finalizeStringAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::STRING);

        if (result.start_time != result.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to produce a string, got multiple values at different times",
                            getPromQLQuery(result, context));
        }

        if (result.store_method != StoreMethod::CONST_STRING)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is of type {} and can't use store method {}",
                            getPromQLQuery(result, context), result.type, result.store_method);
        }

        /// SELECT <start_time> AS time, 'string_value' AS value
        /// [LIMIT ...]
        SelectQueryParams params;

        params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_time_type));
        params.select_list.back()->setAlias(ColumnNames::Time);

        params.select_list.push_back(std::make_shared<ASTLiteral>(result.string_value));
        params.select_list.back()->setAlias(ColumnNames::Value);

        params.limit = context.limit;

        return buildSelectQuery(std::move(params));
    }


    /// Finalizes a SQL query returning an instant vector as three columns "tags", "time", "value".
    ASTPtr finalizeInstantVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::INSTANT_VECTOR);

        if (result.start_time != result.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to produce an instant vector, got multiple vectors at different times",
                            getPromQLQuery(result, context));
        }

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT arrayJoin([]::Array(Array(Tuple(String, String))) AS tags,
                ///        defaultValueOfTypeName(time_type) AS time,
                ///        defaultValueOfTypeName(Float64) AS value
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTLiteral>(Array{}),
                        std::make_shared<ASTLiteral>("Array(Array(Tuple(String, String)))"))));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(
                    makeASTFunction("defaultValueOfTypeName", std::make_shared<ASTLiteral>(context.result_time_type->getName())));
                params.select_list.back()->setAlias(ColumnNames::Time);

                params.select_list.push_back(makeASTFunction("defaultValueOfTypeName", std::make_shared<ASTLiteral>("Float64")));
                params.select_list.back()->setAlias(ColumnNames::Value);

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        <start_time> AS time,
                ///        <scalar_value> AS value
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_time_type));
                params.select_list.back()->setAlias(ColumnNames::Time);

                params.select_list.push_back(std::make_shared<ASTLiteral>(result.scalar_value));
                params.select_list.back()->setAlias(ColumnNames::Value);

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        <start_time> AS time,
                ///        toFloat64(values[1]) AS value
                /// FROM <scalar_grid>
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_time_type));
                params.select_list.back()->setAlias(ColumnNames::Time);

                params.select_list.push_back(makeASTFunction(
                    "toFloat64",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(ColumnNames::Values), std::make_shared<ASTLiteral>(1u))));
                params.select_list.back()->setAlias(ColumnNames::Value);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_table = params.with.back().name;

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        <start_time> AS time,
                ///        toFloat64(values[1]) AS value
                /// FROM <vector_grid>
                /// WHERE isNotNull(values[1])
                /// [ORDER BY tags/value]
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(ColumnNames::Group)));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_time_type));
                params.select_list.back()->setAlias(ColumnNames::Time);

                params.select_list.push_back(makeASTFunction(
                    "toFloat64",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(ColumnNames::Values), std::make_shared<ASTLiteral>(1u))));
                params.select_list.back()->setAlias(ColumnNames::Value);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_table = params.with.back().name;

                params.where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(ColumnNames::Values), std::make_shared<ASTLiteral>(1u)));

                const auto & result_sorting = context.result_sorting;
                if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_TAGS)
                {
                    params.order_by = getOrderByTagsExpressions(result_sorting.sorting_tags);
                    params.order_direction = result_sorting.direction;
                }
                else if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_VALUE)
                {
                    params.order_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Value));
                    params.order_direction = result_sorting.direction;
                }

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Expression {} is of type {} and can't use store method {}",
                                getPromQLQuery(result, context), result.type, result.store_method);
            }
        }

        UNREACHABLE();
    }


    /// Finalizes a SQL query returning a range vector as two columns "tags", "time_series".
    ASTPtr finalizeRangeVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::RANGE_VECTOR);

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT arrayJoin([]::Array(Array(Tuple(String, String)))) AS tags,
                ///        defaultValueOfTypeName(Array(Tuple(time_type, Float64))) AS time_series
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTLiteral>(Array{}),
                        std::make_shared<ASTLiteral>("Array(Array(Tuple(String, String)))"))));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "defaultValueOfTypeName",
                    std::make_shared<ASTLiteral>(fmt::format("Array(Tuple({}, Float64))", context.result_time_type->getName()))));
                params.select_list.back()->setAlias(ColumnNames::TimeSeries);

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           arrayResize([], <count_of_time_steps>, <scalar_value>)) AS time_series
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(result.start_time, context.result_time_type),
                    timeseriesTimeToAST(result.end_time, context.result_time_type),
                    timeseriesDurationToAST(result.step),
                    makeASTFunction(
                        "arrayResize",
                        std::make_shared<ASTLiteral>(Array{}),
                        std::make_shared<ASTLiteral>(countTimeseriesSteps(result.start_time, result.end_time, result.step)),
                        std::make_shared<ASTLiteral>(result.scalar_value))));

                params.select_list.back()->setAlias(ColumnNames::TimeSeries);

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           CAST(values, Array(Float64))) AS time_series
                /// FROM <scalar_grid>
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(result.start_time, context.result_time_type),
                    timeseriesTimeToAST(result.end_time, context.result_time_type),
                    timeseriesDurationToAST(result.step),
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTIdentifier>(ColumnNames::Values),
                        std::make_shared<ASTLiteral>("Array(Float64)"))));

                params.select_list.back()->setAlias(ColumnNames::TimeSeries);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_table = params.with.back().name;

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           CAST(values, Array(Nullable(Float64)))) AS time_series
                /// FROM <vector_grid>
                /// [ORDER BY tags]
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(ColumnNames::Group)));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(result.start_time, context.result_time_type),
                    timeseriesTimeToAST(result.end_time, context.result_time_type),
                    timeseriesDurationToAST(result.step),
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTIdentifier>(ColumnNames::Values),
                        std::make_shared<ASTLiteral>("Array(Nullable(Float64))"))));

                params.select_list.back()->setAlias(ColumnNames::TimeSeries);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_table = params.with.back().name;

                params.where = makeASTFunction("notEmpty", std::make_shared<ASTIdentifier>(ColumnNames::TimeSeries));

                const auto & result_sorting = context.result_sorting;
                if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_TAGS)
                {
                    params.order_by = getOrderByTagsExpressions(result_sorting.sorting_tags);
                    params.order_direction = result_sorting.direction;
                }

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesGroupArray(timestamp, toFloat64(value)) AS time_series
                /// FROM <raw_data>
                /// GROUP BY group
                /// [ORDER BY tags]
                /// [LIMIT ...]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(ColumnNames::Group)));
                params.select_list.back()->setAlias(ColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesGroupArray",
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTIdentifier>(ColumnNames::Timestamp),
                        std::make_shared<ASTLiteral>(context.result_time_type->getName())),
                    makeASTFunction("toFloat64", std::make_shared<ASTIdentifier>(ColumnNames::Value))));

                params.select_list.back()->setAlias(ColumnNames::TimeSeries);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_table = params.with.back().name;

                params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

                const auto & result_sorting = context.result_sorting;
                if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_TAGS)
                {
                    params.order_by = getOrderByTagsExpressions(result_sorting.sorting_tags);
                    params.order_direction = result_sorting.direction;
                }

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_STRING:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Expression {} is of type {} and can't use store method {}",
                                getPromQLQuery(result, context), result.type, result.store_method);
            }
        }

        UNREACHABLE();
    }
}


ASTPtr finalizeSQL(SQLQueryPiece && result, ConverterContext & context)
{
    switch (result.type)
    {
        case ResultType::SCALAR:
            return finalizeScalarAsSQL(std::move(result), context);
        case ResultType::STRING:
            return finalizeStringAsSQL(std::move(result), context);
        case ResultType::INSTANT_VECTOR:
            return finalizeInstantVectorAsSQL(std::move(result), context);
        case ResultType::RANGE_VECTOR:
            return finalizeRangeVectorAsSQL(std::move(result), context);
    }
    UNREACHABLE();
}

}
