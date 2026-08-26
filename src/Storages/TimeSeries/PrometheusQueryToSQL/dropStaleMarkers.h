#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// Replaces Prometheus stale markers with NULL in a `values` array of a grid.
///
/// Grids built for instant selectors intentionally keep stale markers (`fromSelector` passes
/// `filter_stale_markers = false`), so every consumer which treats a grid step as "the series has
/// a value here" must normalize the markers to NULL first. For grids without stale markers - for
/// example grids built for range selectors - this is a no-op.
ASTPtr dropStaleMarkers(ASTPtr values);

}
