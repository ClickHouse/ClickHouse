#pragma once

#include <base/types.h>

namespace DB
{

/// Model constants of the Cascades optimizer that a query cannot change. The per-operator
/// cost constants live in `CostConfig`, which a query can override through
/// `param__internal_cascades_cost_config`; the constants here are fixed.
namespace CascadesDefaults
{

/// The limit on optimization tasks for one query. When the search does not finish within it,
/// the query is rejected instead of built from a partial memo. Only the internal
/// `_internal_cascades_task_limit` parameter can change it.
constexpr size_t DEFAULT_TASK_LIMIT = 100000;

/// Selectivity constants for predicates without usable statistics; the same values that
/// `ConditionSelectivityEstimator` uses for the reads.
constexpr Float64 DEFAULT_EQUALITY_SELECTIVITY = 0.01;
constexpr Float64 DEFAULT_RANGE_SELECTIVITY = 0.33;
constexpr Float64 DEFAULT_UNKNOWN_SELECTIVITY = 0.33;
constexpr Float64 DEFAULT_LIKE_SELECTIVITY = 0.1;

/// Row-width defaults for the exchange and materialization cost terms.
/// Without a minimum, a zero-width row (e.g. a bare `count()`) would make exchanges look free.
constexpr Float64 MIN_ROW_WIDTH = 1.0;
constexpr Float64 DEFAULT_STRING_SIZE = 64.0;
constexpr Float64 DEFAULT_COMPLEX_TYPE_SIZE = 128.0;

}

}
