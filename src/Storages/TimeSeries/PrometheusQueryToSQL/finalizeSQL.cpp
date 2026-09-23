#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>

#include <IO/WriteHelpers.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkSharedSubqueriesAreMaterialized.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    [[noreturn]] void throwCannotFinalize(const SQLQueryPiece & result, const ConverterContext & context)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot finalize expression {} with type: {}, store method: {}, start_time: {}, end_time: {}, step: {}",
                        getPromQLText(result, context),
                        result.type,
                        result.store_method,
                        toString(result.start_time, context.result_timestamp_scale),
                        toString(result.end_time, context.result_timestamp_scale),
                        toString(result.step, context.result_timestamp_scale));
    }

    /// Finalizes a SQL query returning a scalar as two columns "time", "value".
    ASTPtr finalizeScalarAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::SCALAR);

        ASTPtr value;

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                throwCannotFinalize(result, context);
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT <start_time> AS timestamp,
                ///        <scalar_value> AS value

                /// <scalar_value> AS value
                value = timeSeriesScalarToAST(result.scalar_value);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// SELECT <start_time> AS timestamp,
                ///        value::Float64 AS value
                /// FROM <subquery>

                /// value::Float64 AS value
                value = timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value));
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT <start_time> AS timestamp,
                ///        values[1]::Float64 AS value
                /// FROM <scalar_grid>

                /// values[1] AS value
                value = timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::VECTOR_GRID:
            case StoreMethod::RAW_DATA:
            case StoreMethod::HISTOGRAM_RAW_DATA:
            case StoreMethod::HISTOGRAM_GRID:
            {
                /// Can't get in here because these store methods are incompatible with ResultType::SCALAR.
                throwUnexpectedStoreMethod(result, context);
            }
        }

        chassert(value);

        if (result.start_time != result.end_time)
            throwCannotFinalize(result, context);

        /// <start_time> AS timestamp
        ASTPtr timestamp = timeSeriesTimestampToAST(result.start_time, context.result_timestamp_type);
        timestamp->setAlias(ColumnNames::Timestamp);

        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(timestamp));
        builder.select_list.push_back(std::move(value));

        builder.with = std::move(context.subqueries);
        if (result.select_query)
        {
            builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
            builder.from_table = builder.with.back().name;
        }

        return builder.getSelectQuery();
    }


    /// Finalizes a SQL query returning a string as two columns "time", "value".
    ASTPtr finalizeStringAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::STRING);

        /// SELECT <start_time> AS timestamp,
        ///        'string_value' AS value

        if (result.store_method != StoreMethod::CONST_STRING)
        {
            /// Can't get in here because other store methods are incompatible with ResultType::STRING.
            throwUnexpectedStoreMethod(result, context);
        }

        if (result.start_time != result.end_time)
            throwCannotFinalize(result, context);

        ASTPtr timestamp = timeSeriesTimestampToAST(result.start_time, context.result_timestamp_type);
        timestamp->setAlias(ColumnNames::Timestamp);

        ASTPtr value = make_intrusive<ASTLiteral>(result.string_value);
        value->setAlias(ColumnNames::Value);

        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(timestamp));
        builder.select_list.push_back(std::move(value));

        builder.with = std::move(context.subqueries);
        if (result.select_query)
        {
            builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
            builder.from_table = builder.with.back().name;
        }

        return builder.getSelectQuery();
    }


    /// Finalizes a SQL query returning an instant vector as three columns "tags", "time", "value"
    /// (plus a column "histogram" for a combined grid).
    ASTPtr finalizeInstantVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::INSTANT_VECTOR);

        ASTPtr tags;
        ASTPtr value;
        ASTPtr histogram;
        ASTPtr where;

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT * FROM null('tags Array(Tuple(String, String)), timestamp result_timestamp_type, value Float64')
                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTAsterisk>());

                String structure = fmt::format("{} Array(Tuple(String, String)), {} {}, {} Float64",
                    ColumnNames::Tags,
                    ColumnNames::Timestamp, context.result_timestamp_type->getName(),
                    ColumnNames::Value);

                builder.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(std::move(structure)));

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags, <start_time> AS timestamp, <scalar_value> AS value

                /// <scalar_value> AS value
                value = timeSeriesScalarToAST(result.scalar_value);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        <start_time> AS timestamp,
                ///        value::Float64 AS value
                /// FROM <subquery>

                /// value::Float64
                value = timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value));
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        <start_time> AS timestamp,
                ///        values[1]::Float64 AS value
                /// FROM <scalar_grid>

                /// values[1]::Float64 AS value
                value = timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        <start_time> AS timestamp,
                ///        assumeNotNull(values[1])::Float64 AS value
                /// FROM <vector_grid>
                /// WHERE isNotNull(values[1])

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// assumeNotNull(values[1]) AS value
                value = timeSeriesScalarASTCast(
                    makeASTFunction(
                        "assumeNotNull",
                        makeASTFunction(
                            "arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u))));
                value->setAlias(ColumnNames::Value);

                /// WHERE isNotNull(values[1])
                where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));
                break;
            }

            case StoreMethod::HISTOGRAM_GRID:
            {
                /// `sample_kinds[1]` selects the winning arm (0 = float, 1 = histogram): the newest sample of either type wins
                /// (ties keep the histogram), the loser is nulled, so a result row carries exactly one sample (matching Prometheus).

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// if(equals(sample_kinds[1], 0), values[1], NULL) AS value
                value = makeASTFunction(
                    "if",
                    makeASTFunction(
                        "equals",
                        makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTLiteral>(UInt64{0})),
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
                    make_intrusive<ASTLiteral>(Field{}));
                value->setAlias(ColumnNames::Value);

                /// if(equals(sample_kinds[1], 1), histogram_values[1], NULL) AS histogram
                histogram = makeASTFunction(
                    "if",
                    makeASTFunction(
                        "equals",
                        makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTLiteral>(UInt64{1})),
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues), make_intrusive<ASTLiteral>(1u)),
                    make_intrusive<ASTLiteral>(Field{}));
                histogram->setAlias(ColumnNames::Histogram);

                /// WHERE isNotNull(sample_kinds[1])
                where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds), make_intrusive<ASTLiteral>(1u)));
                break;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            case StoreMethod::HISTOGRAM_RAW_DATA:
            {
                /// Can't get in here because these store methods are incompatible with ResultType::INSTANT_VECTOR.
                throwUnexpectedStoreMethod(result, context);
            }
        }

        chassert(value);

        if (!tags)
        {
            /// materialize([]::Array(Tuple(String, String))) AS tags
            tags = makeASTFunction(
                "materialize",
                makeASTFunction("CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Tuple(String, String))")));
            tags->setAlias(ColumnNames::Tags);
        }

        if (result.start_time != result.end_time)
            throwCannotFinalize(result, context);

        /// <start_time> AS timestamp
        ASTPtr timestamp = timeSeriesTimestampToAST(result.start_time, context.result_timestamp_type);
        timestamp->setAlias(ColumnNames::Timestamp);

        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(tags));
        builder.select_list.push_back(std::move(timestamp));
        builder.select_list.push_back(std::move(value));
        if (histogram)
            builder.select_list.push_back(std::move(histogram));

        builder.where = std::move(where);

        builder.with = std::move(context.subqueries);
        if (result.select_query)
        {
            builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
            builder.from_table = builder.with.back().name;
        }

        return builder.getSelectQuery();
    }


    /// Finalizes a SQL query returning a range vector as two columns "tags", "samples"
    /// (plus a column "histogram_series" for a combined grid or a combined selector stream).
    ASTPtr finalizeRangeVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::RANGE_VECTOR);

        const auto * samples_outer_column_name = ColumnNames::getOuterSamples(context.time_series_version);

        ASTPtr tags;
        ASTPtr time_series;
        ASTPtr histogram_series;
        ASTPtr values;
        ASTPtr histogram_values;
        ASTPtr where;
        ASTs group_by;
        ASTPtr having;

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT * FROM null('tags Array(Tuple(String, String)), samples Array(Tuple(result_timestamp_type, Float64))')
                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTAsterisk>());

                String structure = fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, Float64))",
                    ColumnNames::Tags,
                    samples_outer_column_name, context.result_timestamp_type->getName());

                builder.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(std::move(structure)));

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        timeSeriesFromGrid(..., arrayResize([], <count_of_time_steps>, <scalar_value>)) AS samples

                /// arrayResize([], <count_of_time_steps>, <scalar_value>)
                values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(result.start_time, result.end_time, result.step)),
                    timeSeriesScalarToAST(result.scalar_value));
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           arrayResize([], <count_of_time_steps>, value::Float64)) AS samples
                /// FROM <subquery>

                /// arrayResize([], <count_of_time_steps>, value)
                values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(result.start_time, result.end_time, result.step)),
                    timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value)));
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           values::Array(Float64)) AS samples
                /// FROM <scalar_grid>

                /// values::Array(Float64)
                values = makeASTFunction(
                    "CAST",
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTLiteral>("Array(Float64)"));
                break;
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>, values::Array(Nullable(Float64))) AS samples
                /// FROM <vector_grid>
                /// WHERE notEmpty(samples)

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// values::Array(Nullable(Float64))
                values = makeASTFunction(
                    "CAST",
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));

                where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(samples_outer_column_name));
                break;
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        CAST(timeSeriesGroupArray(timestamp, value), 'Array(Tuple(result_timestamp_type, Float64))') AS samples
                /// FROM <raw_data>
                /// GROUP BY group
                /// HAVING notEmpty(samples)

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// CAST(timeSeriesGroupArray(timestamp, value), 'Array(Tuple(result_timestamp_type, Float64))') AS samples
                /// The column `value` of raw data can be Float32 (see the comment for StoreMethod::RAW_DATA),
                /// so we cast the aggregated arrays, which is cheaper than casting the raw data before aggregation.
                time_series = makeASTFunction(
                    "CAST",
                    makeASTFunction("timeSeriesGroupArray", make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), make_intrusive<ASTIdentifier>(ColumnNames::Value)),
                    make_intrusive<ASTLiteral>(fmt::format("Array(Tuple({}, Float64))", context.result_timestamp_type->getName())));
                time_series->setAlias(samples_outer_column_name);

                group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                having = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(samples_outer_column_name));

                break;
            }

            case StoreMethod::HISTOGRAM_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags, timeSeriesFromGrid(...) AS samples, ... AS histogram_series
                /// FROM <histogram_grid> WHERE notEmpty(samples) OR notEmpty(histogram_series)

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// values::Array(Nullable(Float64))
                values = makeASTFunction(
                    "CAST",
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));
                histogram_values = make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues);

                where = makeASTFunction(
                    "or",
                    makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(samples_outer_column_name)),
                    makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::HistogramSeries)));
                break;
            }

            case StoreMethod::HISTOGRAM_RAW_DATA:
            {
                /// `timeSeriesGroupArray` supports only float values, so histogram samples are collected with `groupArrayIf` and sorted
                /// by timestamp afterwards (`timeSeriesFromGrid` is not applicable: a selector stream is not grid-aligned).

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// timeSeriesGroupArrayIf(timestamp, value, equals(is_histogram, 0)) AS samples
                time_series = makeASTFunction(
                    "timeSeriesGroupArrayIf",
                    timeSeriesTimestampASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), context.result_timestamp_type),
                    timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value)),
                    makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{0})));
                time_series->setAlias(samples_outer_column_name);

                ASTs payload_columns;
                for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
                    payload_columns.push_back(make_intrusive<ASTIdentifier>(name));

                auto payload_tuple = makeASTFunction("tuple");
                payload_tuple->arguments->children = std::move(payload_columns);

                /// arraySort(x -> x.1, groupArrayIf(tuple(timestamp, tuple(<payload columns>)), equals(is_histogram, 1))) AS histogram_series
                histogram_series = makeASTFunction(
                    "arraySort",
                    makeASTLambda({"x"}, makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(1u))),
                    makeASTFunction(
                        "groupArrayIf",
                        makeASTFunction(
                            "tuple",
                            timeSeriesTimestampASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), context.result_timestamp_type),
                            std::move(payload_tuple)),
                        makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{1}))));
                histogram_series->setAlias(ColumnNames::HistogramSeries);

                group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                having = makeASTFunction(
                    "or",
                    makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(samples_outer_column_name)),
                    makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::HistogramSeries)));

                break;
            }

            case StoreMethod::CONST_STRING:
            {
                /// Can't get in here because this store method are incompatible with ResultType::RANGE_VECTOR.
                throwUnexpectedStoreMethod(result, context);
            }
        }

        chassert(time_series || values);

        if (!tags)
        {
            /// materialize([]::Array(Tuple(String, String))) AS tags
            tags = makeASTFunction(
                "materialize",
                makeASTFunction("CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Tuple(String, String))")));
            tags->setAlias(ColumnNames::Tags);
        }

        if (!time_series)
        {
            /// timeSeriesFromGrid(<start_time>, <end_time>, <step>, <values>) AS samples
            chassert(values);
            time_series = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.result_timestamp_type),
                    timeSeriesTimestampToAST(result.end_time, context.result_timestamp_type),
                    timeSeriesDurationToAST(result.step, context.result_timestamp_type),
                    std::move(values));
            time_series->setAlias(samples_outer_column_name);
        }

        if (histogram_values)
        {
            /// timeSeriesFromGrid(<start_time>, <end_time>, <step>, <histogram_values>) AS histogram_series
            histogram_series = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.result_timestamp_type),
                    timeSeriesTimestampToAST(result.end_time, context.result_timestamp_type),
                    timeSeriesDurationToAST(result.step, context.result_timestamp_type),
                    std::move(histogram_values));
            histogram_series->setAlias(ColumnNames::HistogramSeries);
        }

        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(tags));
        builder.select_list.push_back(std::move(time_series));
        if (histogram_series)
            builder.select_list.push_back(std::move(histogram_series));

        builder.where = std::move(where);
        builder.group_by = std::move(group_by);
        builder.having = std::move(having);

        /// Data from range queries comes sorted alphabetically by tags.
        builder.order_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));
        builder.order_direction = 1;

        builder.with = std::move(context.subqueries);
        if (result.select_query)
        {
            builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
            builder.from_table = builder.with.back().name;
        }

        return builder.getSelectQuery();
    }

}


ASTPtr finalizeSQL(SQLQueryPiece && result, ConverterContext & context)
{
    ASTPtr res;

    switch (result.type)
    {
        case ResultType::SCALAR:
            res = finalizeScalarAsSQL(std::move(result), context);
            break;
        case ResultType::STRING:
            res = finalizeStringAsSQL(std::move(result), context);
            break;
        case ResultType::INSTANT_VECTOR:
            res = finalizeInstantVectorAsSQL(std::move(result), context);
            break;
        case ResultType::RANGE_VECTOR:
            res = finalizeRangeVectorAsSQL(std::move(result), context);
            break;
    }

    if (!res)
        UNREACHABLE();

    checkSharedSubqueriesAreMaterialized(res);

    return res;
}

}
