#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <Common/Exception.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/resolveMetricNameDrop.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.metric_name_dropped || query_piece.metric_name_drop_pending)
        return std::move(query_piece);

    switch (query_piece.store_method)
    {
        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// No metric name.
            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// When we remove the metric name `__name__` it's possible that we get the same set of tags (i.e. the same `group`)
            /// on time series which were different before we removed the metric name.
            /// Prometheus evaluates each step independently, so this is allowed only if such series don't have values
            /// at the same step (for example, if a metric was renamed at some point), and then their values are merged into one series.
            /// Otherwise it's an error, we can't have multiple time series with the same set of tags in the same resultset.
            ///
            /// Example:
            ///             tags                           timestamp1        timestamp2
            /// metric1{tag1='value1', tag2='value2'}       value_a           value_b
            /// metric2{tag1='value1', tag2='value2'}       value_c           value_d
            ///                                 ||
            ///                                 \/
            ///             tags                           timestamp1        timestamp2
            /// {tag1='value1', tag2='value2'}              value_a           value_b
            /// {tag1='value1', tag2='value2'}              value_c           value_d
            ///
            /// That's why we need the function timeSeriesThrowDuplicateSeriesIf() to detect such cases and throw an exception.
            ///
            /// The check regroups every series through anyForEach() and countForEach(), which costs as much as
            /// the aggregation that usually follows (e.g. `sum(rate(...))`). So for a grid with more than one step the drop
            /// is deferred: the consumer of the grid either folds it into its own aggregation (an element-wise aggregation
            /// over series with the same metric name can't produce duplicates) or skips the check when all the series
            /// have the same metric name, see resolveMetricNameDrop().
            if (stepsInTimeSeriesRange(query_piece.start_time, query_piece.end_time, query_piece.step) > 1)
            {
                query_piece.metric_name_drop_pending = true;
                return std::move(query_piece);
            }

            query_piece.select_query = dropMetricNameAndMergeDuplicates(std::move(query_piece.select_query), context);
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::RAW_DATA:
        {
            /// dropMetricName() must not be called with StoreMethod::RAW_DATA.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot drop the metric name from the result of expression {} because of its store method {}",
                            getPromQLText(query_piece, context), query_piece.store_method);
        }
    }

    UNREACHABLE();
}

}
