#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionArguments.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <base/defines.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

ASTs getToGridAggregateFunctionArguments(const SQLQueryPiece & range_vector, ConverterContext & context)
{
    chassert(range_vector.type == ResultType::RANGE_VECTOR);

    ASTPtr timestamps;
    ASTPtr values;

    switch (range_vector.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        {
            /// values: arrayResize([], <count_of_time_steps>, <scalar_value>)
            /// where <scalar_value> is a literal or the `value` column of the single-row subquery.
            ASTPtr value = (range_vector.store_method == StoreMethod::CONST_SCALAR)
                ? timeSeriesScalarToAST(range_vector.scalar_value)
                : make_intrusive<ASTIdentifier>(ColumnNames::Value);

            values = makeASTFunction(
                "arrayResize",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(range_vector.start_time, range_vector.end_time, range_vector.step)),
                std::move(value));

            break;
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// values: the `values` column of the scalar grid
            values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// timestamps: (timeSeriesFromGrid(<start_time>, <end_time>, <step>, values) AS samples).1
            /// values:     samples.2
            ASTPtr samples = makeASTFunction(
                "timeSeriesFromGrid",
                timeSeriesTimestampToAST(range_vector.start_time, context.result_timestamp_type),
                timeSeriesTimestampToAST(range_vector.end_time, context.result_timestamp_type),
                timeSeriesDurationToAST(range_vector.step, context.result_timestamp_type),
                make_intrusive<ASTIdentifier>(ColumnNames::Values));
            samples->setAlias(ColumnNames::Samples);
            timestamps = makeASTFunction("tupleElement", std::move(samples), make_intrusive<ASTLiteral>(1));
            values = makeASTFunction(
                "tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::Samples), make_intrusive<ASTLiteral>(2));

            break;
        }

        case StoreMethod::RAW_DATA:
        {
            if (context.time_series_version >= TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES)
            {
                /// Bucketed tables expose each input row as an array of `(timestamp, value)` samples.
                return {make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries)};
            }

            /// Older tables preserve the row layout used by the historical SQL translation.
            timestamps = make_intrusive<ASTIdentifier>(ColumnNames::Timestamp);
            values = make_intrusive<ASTIdentifier>(ColumnNames::Value);
            break;
        }

        case StoreMethod::EMPTY:
        {
            /// The callers must handle an empty range vector themselves.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "getToGridAggregateFunctionArguments: Expression {} is empty",
                            getPromQLText(range_vector, context));
        }

        case StoreMethod::CONST_STRING:
        {
            /// Can't get in here because the store method CONST_STRING is incompatible with a range vector.
            throwUnexpectedStoreMethod(range_vector, context);
        }
    }

    if (!timestamps)
    {
        /// The values of a scalar are aligned to the grid, so their timestamps are the grid itself:
        /// timestamps: timeSeriesRange(<start_time>, <end_time>, <step>)
        timestamps = makeASTFunction(
            "timeSeriesRange",
            timeSeriesTimestampToAST(range_vector.start_time, context.result_timestamp_type),
            timeSeriesTimestampToAST(range_vector.end_time, context.result_timestamp_type),
            timeSeriesDurationToAST(range_vector.step, context.result_timestamp_type));
    }

    return {std::move(timestamps), std::move(values)};
}

}
