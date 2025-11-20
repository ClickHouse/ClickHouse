#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece toVectorGrid(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.type != ResultType::INSTANT_VECTOR)
    {
        /// toVectorGrid() can only be called for an instant vector.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Expression {} has unexpected type {}, expected: {}",
                        getPromQLQuery(query_piece, context), query_piece.type, ResultType::INSTANT_VECTOR);
    }

    switch (query_piece.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            /// SELECT 0 AS group,
            ///        arrayResize([], <count_of_time_steps>, <scalar_value>) AS values
            SelectQueryParams params;
            params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
            params.select_list.back()->setAlias(ColumnNames::Group);

            params.select_list.push_back(makeASTFunction(
                "arrayResize",
                std::make_shared<ASTLiteral>(Array{}),
                std::make_shared<ASTLiteral>(countTimeseriesSteps(query_piece.start_time, query_piece.end_time, query_piece.step)),
                std::make_shared<ASTLiteral>(query_piece.scalar_value)));

            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Values));

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            params.from_table = context.subqueries.back().name;

            query_piece.select_query = buildSelectQuery(std::move(params));
            query_piece.store_method = StoreMethod::VECTOR_GRID;
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// SELECT 0 AS group,
            ///        values
            /// FROM <scalar_grid>
            SelectQueryParams params;
            params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
            params.select_list.back()->setAlias(ColumnNames::Group);

            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Values));

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            params.from_table = context.subqueries.back().name;

            query_piece.select_query = buildSelectQuery(std::move(params));
            query_piece.store_method = StoreMethod::VECTOR_GRID;
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::VECTOR_GRID:
        {
            return std::move(query_piece);
        }

        case StoreMethod::EMPTY:
        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// toVectorGrid() can only be called with store method CONST_SCALAR or SCALAR_GRID or VECTOR_GRID.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} has unexpected store_method: {}, can't convert it to {}",
                            getPromQLQuery(query_piece, context), query_piece.store_method, StoreMethod::VECTOR_GRID);
        }
    }

    UNREACHABLE();
}

}
