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
            /// Removes __name__ and checks for duplicate series after tag removal.
            /// Groups by group alias directly to eliminate an extra renaming subquery.
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction(
                "timeSeriesRemoveTag", make_intrusive<ASTIdentifier>(ColumnNames::Group), make_intrusive<ASTLiteral>(kMetricName)));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            builder.select_list.push_back(makeASTFunction("any", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            builder.having = makeASTFunction(
                "equals",
                makeASTFunction(
                    "timeSeriesThrowDuplicateSeriesIf",
                    makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                    make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                make_intrusive<ASTLiteral>(0u));

            query_piece.select_query = builder.getSelectQuery();
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
