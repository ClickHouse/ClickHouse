#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


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
                        "toVectorGrid: Expression {} has unexpected type {}, expected: {}",
                        getPromQLText(query_piece, context), query_piece.type, ResultType::INSTANT_VECTOR);
    }

    switch (query_piece.store_method)
    {
        case StoreMethod::EMPTY:
        {
            /// SELECT * FROM null('group UInt64, values Array(Nullable(scalar_data_type))')
            SelectQueryBuilder builder;
            builder.select_list.push_back(make_intrusive<ASTAsterisk>());

            String structure = fmt::format("{} UInt64, {} Array(Nullable({}))",
                ColumnNames::Group,
                ColumnNames::Values,
                context.scalar_data_type->getName());

            builder.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(std::move(structure)));

            query_piece.select_query = builder.getSelectQuery();
            query_piece.store_method = StoreMethod::VECTOR_GRID;
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        {
            /// For const scalar:
            /// SELECT 0 AS group, arrayResize([], <count_of_time_steps>, <scalar_value>) AS values
            ///
            /// For single scalar:
            /// SELECT 0 AS group, arrayResize([], <count_of_time_steps>, value) AS values FROM <subquery>
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTLiteral>(0u));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            ASTPtr value = (query_piece.store_method == StoreMethod::CONST_SCALAR)
                ? timeSeriesScalarToAST(query_piece.scalar_value, context.scalar_data_type)
                : make_intrusive<ASTIdentifier>(ColumnNames::Value);

            builder.select_list.push_back(makeASTFunction(
                "arrayResize",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(query_piece.start_time, query_piece.end_time, query_piece.step)),
                std::move(value)));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            if (query_piece.select_query)
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;
            }

            query_piece.select_query = builder.getSelectQuery();
            query_piece.store_method = StoreMethod::VECTOR_GRID;
            query_piece.scalar_value = {};
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// SELECT 0 AS group, values
            /// FROM <scalar_grid>
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTLiteral>(0u));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            query_piece.select_query = builder.getSelectQuery();
            query_piece.store_method = StoreMethod::VECTOR_GRID;
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::VECTOR_GRID:
        {
            return std::move(query_piece);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// toVectorGrid() can only be called with store method CONST_SCALAR or SCALAR_GRID or VECTOR_GRID.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "toVectorGrid: Cannot convert expression {} to {} because of its store method {}",
                            getPromQLText(query_piece, context), StoreMethod::VECTOR_GRID, query_piece.store_method);
        }
    }

    UNREACHABLE();
}

}
