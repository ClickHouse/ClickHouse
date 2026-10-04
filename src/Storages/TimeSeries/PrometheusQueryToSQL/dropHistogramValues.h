#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Drops the histogram arm of a StoreMethod::HISTOGRAM_GRID piece and turns it into StoreMethod::VECTOR_GRID: a step keeps its float
/// value only if the newest sample there is a float (see `sample_kinds`), and a row without such a step is dropped.
/// Must be called only with StoreMethod::HISTOGRAM_GRID.
SQLQueryPiece dropHistogramValues(SQLQueryPiece && query_piece, ConverterContext & context);

/// Reduces a piece that can carry native-histogram samples to its float samples only, so that a function which is
/// defined on float samples only (Prometheus ignores the histogram samples there) can consume it. Pieces stored
/// without histograms are returned unchanged.
SQLQueryPiece dropHistogramSamples(SQLQueryPiece && query_piece, ConverterContext & context);

}
