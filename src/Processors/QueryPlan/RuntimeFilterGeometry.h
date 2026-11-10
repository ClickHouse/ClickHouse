#pragma once
#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>

namespace DB
{

struct QueryPlanSerializationSettings;
class ReadBuffer;
class WriteBuffer;

/// First plan version that can carry runtime filter exchanges (step version 1 of `BuildRuntimeFilter`),
/// the `MergeRuntimeFilters` step and the `join_runtime_filter_exact_bytes_limit` plan setting.
static constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES = 20;
/// First plan version that writes step version 2 of `BuildRuntimeFilter`, which carries the rendezvous key
/// of a local build too. A receiver at step version 1 requires a key exactly when the step has exchanges.
static constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LOCAL_RUNTIME_FILTER_KEY = 21;

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

    void write(WriteBuffer & out) const;
    static RuntimeFilterGeometry read(ReadBuffer & in);

    /// Throws if a geometry read from the wire is out of bounds.
    void validateTransported() const;
};

}
