#pragma once

#include <base/types.h>
#include <Columns/IColumn_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <string_view>


namespace DB
{

/// The "locality_hash" column contains a hash of the metric name, `xxHash64(metric_name)`.
/// In the "samples" table it's used as the first column of the primary key
/// to store samples of the same metric close to each other; the write paths
/// (INSERT into a TimeSeries table and the Prometheus remote-write protocol) calculate it
/// with calculateTimeSeriesLocalityHash().
/// In the "tags" table it's a MATERIALIZED column with the expression made by
/// makeTimeSeriesLocalityHashAST(), so that the SQL queries generated for evaluating
/// PromQL selectors can read it instead of calculating any hashes.
/// The functions below are the single source of truth for the invariant
/// `samples.locality_hash = tags.locality_hash = xxHash64(metric_name)`.

/// Calculates the locality hash of a single metric name.
UInt64 calculateTimeSeriesLocalityHash(std::string_view metric_name);

/// Makes a UInt64 column with locality hashes calculated from a column containing metric names.
ColumnPtr buildTimeSeriesLocalityHashColumn(const IColumn & metric_name_column);

/// Makes an AST for the SQL expression `xxHash64(metric_name)` which calculates
/// the same hash as calculateTimeSeriesLocalityHash(). It's used as the MATERIALIZED
/// expression of the `locality_hash` column in the "tags" table.
ASTPtr makeTimeSeriesLocalityHashAST();

}
