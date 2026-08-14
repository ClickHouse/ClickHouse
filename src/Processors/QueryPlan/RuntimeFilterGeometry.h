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

/// Sizing and self-disabling parameters of one runtime filter, shared by every party that builds,
/// transports, merges, or probes it. Partial filter states are only mergeable when built to
/// identical geometry, and the receiving side validates arrived state against the plan's values,
/// so the build, send, and receive steps of one filter must carry the same geometry.
struct RuntimeFilterGeometry
{
    /// The filter keeps the exact key set until it exceeds either limit, then degrades to a bloom
    /// filter of `bloom_filter_bytes`. For a local filter `exact_bytes_limit` equals
    /// `bloom_filter_bytes`; for a transported (cross-task) filter the plan may raise it above the
    /// bloom size — capped at `MAX_RUNTIME_BLOOM_FILTER_BYTES` — so an estimate-sized key set can
    /// arrive exact while a degraded partial still costs no more than the settings-sized bloom.
    UInt64 exact_values_limit = 0;
    UInt64 exact_bytes_limit = 0;
    UInt64 bloom_filter_bytes = 0;
    UInt64 bloom_filter_hash_functions = 0;

    /// See `join_runtime_filter_pass_ratio_threshold_for_disabling` and related settings.
    Float64 pass_ratio_threshold_for_disabling = 0.7;
    UInt64 blocks_to_skip_before_reenabling = 30;
    Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;

    /// The field defaults are not a usable geometry: the sizing fields default to 0 = unset
    /// (normalized and bound-checked only by the `BuildRuntimeFilterStep` constructor), and the
    /// disabling knobs merely mirror their settings' defaults. Every construction site
    /// (`fromSettings`, the join optimizer, tests) sets all fields explicitly; a new one must too.

    void serializeSettings(QueryPlanSerializationSettings & settings) const;
    static RuntimeFilterGeometry fromSettings(const QueryPlanSerializationSettings & settings);

    /// Bounds check for a geometry that arrived with a serialized plan. The building side
    /// normalizes and validates in the `BuildRuntimeFilterStep` constructor; a transported
    /// send/receive step instead rejects a plan whose geometry that constructor and the
    /// transport sizing (settings floor, `MAX_RUNTIME_BLOOM_FILTER_BYTES` cap) could not have
    /// produced.
    void validateTransported() const;
};

}
