#pragma once
#include <base/types.h>

namespace DB
{

struct QueryPlanSerializationSettings;

/// Runtime bloom filter should be small and fast otherwise it is pointless
static constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;
static constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 10;
static constexpr UInt64 DEFAULT_RUNTIME_BLOOM_FILTER_BYTES = 512 * 1024;
static constexpr UInt64 DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 3;

/// First query-plan serialization version that knows `join_runtime_filter_exact_bytes_limit` and
/// `BuildRuntimeFilterStep` filter-exchange topology. Gates writing the setting in `serializeSettings`.
static constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES = 10;

/// Sizing + self-disable knobs. Partials only merge if geometry matches; the receiver
/// checks arrived state against the plan.
struct RuntimeFilterGeometry
{
    UInt64 exact_values_limit = 0;
    /// Local: `exact_bytes_limit == bloom_filter_bytes`. Transported: plan may raise it,
    /// still capped at `MAX_RUNTIME_BLOOM_FILTER_BYTES`.
    UInt64 exact_bytes_limit = 0;
    UInt64 bloom_filter_bytes = 0;
    UInt64 bloom_filter_hash_functions = 0;

    /// See `join_runtime_filter_pass_ratio_threshold_for_disabling` and related settings.
    Float64 pass_ratio_threshold_for_disabling = 0.7;
    UInt64 blocks_to_skip_before_reenabling = 30;
    Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;

    /// Sizing fields default to 0 = unset. The `BuildRuntimeFilterStep` constructor floors a
    /// 0 `exact_bytes_limit` to `bloom_filter_bytes`; the join optimizer leaves that field unset.

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const;
    static RuntimeFilterGeometry fromSettings(const QueryPlanSerializationSettings & settings);

    /// Reject a serialized geometry the `BuildRuntimeFilterStep` constructor plus transport
    /// sizing (settings floor, `MAX_RUNTIME_BLOOM_FILTER_BYTES`) could not have produced.
    void validateTransported() const;
};

}
