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
            ///             tags                                   timestamp1        timestamp2
            /// metric1{tag1='value1', tag2='value2', ''='1'}       value_a           value_b
            /// metric2{tag1='value1', tag2='value2', ''='1'}       value_c           value_d
            ///
            /// finalizeSQL removes both `__name__` and the marker, and throws an exception if series collide after that.

            /// SELECT if(isNotNull(timeSeriesExtractTag(group, '__name__')), timeSeriesReplaceTag(group, '', '1', '', ''), group) AS group,
            ///        values
            /// FROM <vector_grid>
            ///
            /// The empty source tag is a missing tag, and the empty regular expression matches its empty value,
            /// so the marker is written to every series which has a metric name, whatever bytes that name contains.
            /// The `if` reads the source column `group`. Its alias is the output column of the same name.
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "if",
                    makeASTFunction(
                        "isNotNull",
                        makeASTFunction(
                            "timeSeriesExtractTag",
                            make_intrusive<ASTIdentifier>(ColumnNames::Group),
                            make_intrusive<ASTLiteral>(kMetricName))),
                    makeASTFunction(
                        "timeSeriesReplaceTag",
                        make_intrusive<ASTIdentifier>(ColumnNames::Group),
                        make_intrusive<ASTLiteral>(kDroppedMetricNameMarker),
                        make_intrusive<ASTLiteral>(kDroppedMetricNameMarkerValue),
                        make_intrusive<ASTLiteral>(String{}),
                        make_intrusive<ASTLiteral>(String{})),
                    make_intrusive<ASTIdentifier>(ColumnNames::Group)));
                builder.select_list.back()->setAlias(ColumnNames::Group);

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                query_piece.select_query = builder.getSelectQuery();
            }
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
