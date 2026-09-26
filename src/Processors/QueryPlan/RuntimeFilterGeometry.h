#pragma once
#include <base/types.h>

namespace DB
{

struct QueryPlanSerializationSettings;

/// Runtime bloom filter should be small and fast otherwise it is pointless
static constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;
static constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 10;

/// The global query-plan version of the release that introduced the runtime filter transport. Step
/// version 1 of `BuildRuntimeFilter` (the filter exchange topology) is written from this plan version
/// on, and so is the `join_runtime_filter_exact_bytes_limit` plan setting, which a peer below it does
/// not know. This version also introduces the `MergeRuntimeFilters` step.
static constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES = 20;

/// Sizing and self-disabling parameters of a runtime filter. Partials merge only when their geometry
/// matches, and a receiver checks each arrived state against the plan's geometry.
struct RuntimeFilterGeometry
{
    UInt64 exact_values_limit = 0;
    /// The join optimizer leaves it 0, and the `BuildRuntimeFilterStep` constructor sets it to
    /// `bloom_filter_bytes`. Transport may raise it from row estimates, up to `MAX_RUNTIME_BLOOM_FILTER_BYTES`.
    UInt64 exact_bytes_limit = 0;
    /// For both bloom fields, 0 selects the default; the `BuildRuntimeFilterStep` constructor resolves it.
    UInt64 bloom_filter_bytes = 0;
    UInt64 bloom_filter_hash_functions = 0;

    /// See `join_runtime_filter_pass_ratio_threshold_for_disabling` and related settings.
    Float64 pass_ratio_threshold_for_disabling = 0.7;
    UInt64 blocks_to_skip_before_reenabling = 30;
    Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const;
    static RuntimeFilterGeometry fromSettings(const QueryPlanSerializationSettings & settings);

    /// Reject a serialized geometry the `BuildRuntimeFilterStep` constructor plus transport
    /// sizing (settings floor, `MAX_RUNTIME_BLOOM_FILTER_BYTES`) could not have produced.
    void validateTransported() const;
};

}
