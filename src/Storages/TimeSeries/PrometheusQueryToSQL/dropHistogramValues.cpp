#include <Storages/TimeSeries/PrometheusQueryToSQL/dropHistogramValues.h>

#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece dropHistogramValues(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.store_method != StoreMethod::HISTOGRAM_GRID)
    {
        /// dropHistogramValues must be called only with StoreMethod::HISTOGRAM_GRID.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot drop the histogram values from the result of expression {} because of its store method {}",
                        getPromQLText(query_piece, context), query_piece.store_method);
    }

    /// SELECT group, values
    /// FROM <histogram_grid>
    SelectQueryBuilder builder;

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
    builder.from_table = context.subqueries.back().name;

    query_piece.select_query = builder.getSelectQuery();
    query_piece.store_method = StoreMethod::VECTOR_GRID;

    return std::move(query_piece);
}

}
