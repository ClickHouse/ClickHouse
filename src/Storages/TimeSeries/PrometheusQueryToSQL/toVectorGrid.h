#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{
    
/// Converts a query piece of type INSTANT_VECTOR to StoreMethod::VECTOR_GRID.
/// We use that when we need to aggregate by tags.
/// The argument must be an instant vector using one of the following store methods:
/// CONST_SCALAR, SCALAR_GRID, VECTOR_GRID.
SQLQueryPiece toVectorGrid(SQLQueryPiece && query_piece, ConverterContext & context);

}
