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
            /// When we remove the metric name `__name__` it's possible that we get the same set of tags (i.e. the same `group`)
            /// on time series which were different before we removed the metric name.
            /// This is not allowed, we can't have multiple time series with the same set of tags in the same resultset.
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

            /// Step 1:
            /// SELECT timeSeriesRemoveTag(group, '__name__') AS new_group,
            ///        any(values) AS values
            /// FROM <vector_grid>
            /// GROUP BY new_group
            /// HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, new_group) = 0
            ASTPtr metric_name_removing_query;
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesRemoveTag", make_intrusive<ASTIdentifier>(ColumnNames::Group), make_intrusive<ASTLiteral>(kMetricName)));
                builder.select_list.back()->setAlias(ColumnNames::NewGroup);

                builder.select_list.push_back(makeASTFunction("any", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
                builder.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)),
                    make_intrusive<ASTLiteral>(0u));

                metric_name_removing_query = builder.getSelectQuery();
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

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(metric_name_removing_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                column_renaming_query = builder.getSelectQuery();
            }

            query_piece.select_query = std::move(column_renaming_query);
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
