#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>

#include <IO/WriteHelpers.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
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
    [[noreturn]] void throwCannotFinalize(const SQLQueryPiece & result, const ConverterContext & context)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot finalize expression {} with type: {}, store method: {}, start_time: {}, end_time: {}, step: {}",
                        getPromQLText(result, context),
                        result.type,
                        result.store_method,
                        toString(result.start_time, context.timestamp_scale),
                        toString(result.end_time, context.timestamp_scale),
                        toString(result.step, context.timestamp_scale));
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
                value = timeSeriesScalarToAST(result.scalar_value, context.scalar_data_type);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// SELECT <start_time> AS timestamp,
                ///        value::scalar_data_type AS value
                /// FROM <subquery>

                /// value::scalar_data_type AS value
                value = timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value), context.scalar_data_type);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT <start_time> AS timestamp,
                ///        values[1]::scalar_data_type AS value
                /// FROM <scalar_grid>

                /// values[1] AS value
                value = timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
                    context.scalar_data_type);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::VECTOR_GRID:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because these store methods are incompatible with ResultType::SCALAR.
                throwUnexpectedStoreMethod(result, context);
            }
        }

        chassert(value);

        if (result.start_time != result.end_time)
            throwCannotFinalize(result, context);

        /// <start_time> AS timestamp
        ASTPtr timestamp = timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type);
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

        ASTPtr timestamp = timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type);
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


    /// Finalizes a SQL query returning an instant vector as three columns "tags", "time", "value".
    ASTPtr finalizeInstantVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::INSTANT_VECTOR);

        ASTPtr tags;
        ASTPtr value;
        ASTPtr where;

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT * FROM null('tags Array(Tuple(String, String)), timestamp timestamp_data_type, value scalar_data_type')
                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTAsterisk>());

                String structure = fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                    ColumnNames::Tags,
                    ColumnNames::Timestamp, context.timestamp_data_type->getName(),
                    ColumnNames::Value, context.scalar_data_type->getName());

                builder.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(std::move(structure)));

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        <start_time> AS timestamp,
                ///        <scalar_value> AS value

                /// <scalar_value> AS value
                value = timeSeriesScalarToAST(result.scalar_value, context.scalar_data_type);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        <start_time> AS timestamp,
                ///        value::scalar_data_type AS value
                /// FROM <subquery>

                /// value::scalar_data_type
                value = timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value), context.scalar_data_type);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        <start_time> AS timestamp,
                ///        values[1]::scalar_data_type AS value
                /// FROM <scalar_grid>

                /// values[1]::scalar_data_type AS value
                value = timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
                    context.scalar_data_type);
                value->setAlias(ColumnNames::Value);
                break;
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        <start_time> AS timestamp,
                ///        assumeNotNull(values[1])::scalar_data_type AS value
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
                            "arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u))),
                    context.scalar_data_type);
                value->setAlias(ColumnNames::Value);

                /// WHERE isNotNull(values[1])
                where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));
                break;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
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
        ASTPtr timestamp = timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type);
        timestamp->setAlias(ColumnNames::Timestamp);

        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(tags));
        builder.select_list.push_back(std::move(timestamp));
        builder.select_list.push_back(std::move(value));

        builder.where = std::move(where);

        builder.with = std::move(context.subqueries);
        if (result.select_query)
        {
            builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
            builder.from_table = builder.with.back().name;
        }

        return builder.getSelectQuery();
    }


    /// Finalizes a SQL query returning a range vector as two columns "tags", "time_series".
    ASTPtr finalizeRangeVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::RANGE_VECTOR);

        ASTPtr tags;
        ASTPtr time_series;
        ASTPtr values;
        ASTPtr where;
        ASTs group_by;
        ASTPtr having;

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT * FROM null('tags Array(Tuple(String, String)), time_series Array(Tuple(timestamp_data_type, scalar_data_type))')
                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTAsterisk>());

                String structure = fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, {}))",
                    ColumnNames::Tags,
                    ColumnNames::TimeSeries, context.timestamp_data_type->getName(), context.scalar_data_type->getName());

                builder.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(std::move(structure)));

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           arrayResize([], <count_of_time_steps>, <scalar_value>)) AS time_series

                /// arrayResize([], <count_of_time_steps>, <scalar_value>)
                values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(result.start_time, result.end_time, result.step)),
                    timeSeriesScalarToAST(result.scalar_value, context.scalar_data_type));
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           arrayResize([], <count_of_time_steps>, value::scalar_data_type)) AS time_series
                /// FROM <subquery>

                /// arrayResize([], <count_of_time_steps>, value)
                values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(result.start_time, result.end_time, result.step)),
                    timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value), context.scalar_data_type));
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT materialize([]::Array(Tuple(String, String))) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           values::Array(scalar_data_type)) AS time_series
                /// FROM <scalar_grid>

                /// values::Array(scalar_data_type)
                values = makeASTFunction(
                    "CAST",
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTLiteral>(fmt::format("Array({})", context.scalar_data_type->getName())));
                break;
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>, values::Array(Nullable(scalar_data_type))) AS time_series
                /// FROM <vector_grid>
                /// WHERE notEmpty(time_series)

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// values::Array(Nullable(scalar_data_type))
                values = makeASTFunction(
                    "CAST",
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTLiteral>(fmt::format("Array(Nullable({}))", context.scalar_data_type->getName())));

                where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries));
                break;
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesGroupArray(timestamp::timestamp_data_type, value::scalar_data_type) AS time_series
                /// FROM <raw_data>
                /// GROUP BY group
                /// HAVING notEmpty(time_series)

                /// timeSeriesGroupToTags(group) AS tags
                tags = makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
                tags->setAlias(ColumnNames::Tags);

                /// timeSeriesGroupArray(timestamp, value) AS time_series
                time_series = makeASTFunction(
                    "timeSeriesGroupArray",
                    timeSeriesTimestampASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), context.timestamp_data_type),
                    timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value), context.scalar_data_type));
                time_series->setAlias(ColumnNames::TimeSeries);

                group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                having = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries));

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
            /// timeSeriesFromGrid(<start_time>, <end_time>, <step>, <values>) AS time_series
            chassert(values);
            time_series = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(result.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(result.step, context.timestamp_data_type),
                    std::move(values));
            time_series->setAlias(ColumnNames::TimeSeries);
        }

        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(tags));
        builder.select_list.push_back(std::move(time_series));

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
