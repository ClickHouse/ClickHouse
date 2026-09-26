#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.metric_name_dropped)
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
            /// The metric name `__name__` is not removed here, it's removed from the final result by finalizeSQL.
            /// Here we only mark the series which have a metric name with the tag `kDroppedMetricNameMarker`.
            /// This is the same as the delayed name removal in Prometheus: series which differ only by the metric name
            /// stay distinct in intermediate results, and functions like label_replace can still read `__name__`.
            ///
            /// Example:
            ///             tags                           timestamp1        timestamp2
            /// metric1{tag1='value1', tag2='value2'}       value_a           value_b
            /// metric2{tag1='value1', tag2='value2'}       value_c           value_d
            ///                                 ||
            ///                                 \/
            ///             tags                                                   timestamp1        timestamp2
            /// metric1{tag1='value1', tag2='value2', __name__.dropped='1'}          value_a           value_b
            /// metric2{tag1='value1', tag2='value2', __name__.dropped='1'}          value_c           value_d
            ///
            /// finalizeSQL removes both `__name__` and the marker, and throws an exception if series collide after that.

            /// Step 1:
            /// SELECT timeSeriesReplaceTag(group, '__name__.dropped', '1', '__name__', '.+') AS new_group,
            ///        values
            /// FROM <vector_grid>
            ///
            /// The regular expression '.+' doesn't match an empty string, so a series without a metric name is left unchanged.
            ASTPtr metric_name_marking_query;
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesReplaceTag",
                    make_intrusive<ASTIdentifier>(ColumnNames::Group),
                    make_intrusive<ASTLiteral>(kDroppedMetricNameMarker),
                    make_intrusive<ASTLiteral>(kDroppedMetricNameMarkerValue),
                    make_intrusive<ASTLiteral>(kMetricName),
                    make_intrusive<ASTLiteral>(".+")));
                builder.select_list.back()->setAlias(ColumnNames::NewGroup);

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                metric_name_marking_query = builder.getSelectQuery();
            }

            /// Step 2:
            /// SELECT new_group AS group, values
            /// FROM step1
            ASTPtr column_renaming_query;
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
                builder.select_list.back()->setAlias(ColumnNames::Group);

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(metric_name_marking_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                column_renaming_query = builder.getSelectQuery();
            }

            query_piece.select_query = std::move(column_renaming_query);
            context.metric_name_drop_deferred = true;

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
