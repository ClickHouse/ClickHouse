#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct SerializedSetsRegistry;
struct DeserializedSetsRegistry;

/// Serialization context passed to `IQueryPlanStep::serialize`.
/// Settings are handled separately via `serializeSettings` method.
struct IQueryPlanStep::Serialization
{
    WriteBuffer & out;
    SerializedSetsRegistry & registry;

    // Set when a step is serialized to compute its Auto-PR plan cache-key hash (not for transmission).
    // In that mode the serialization omits fields that would otherwise break hash matching between the
    // single-node and distributed (parallel-replicas) plan builds: `AggregatingStep`'s `final` flag
    // (which differs between those builds) and its stats-collecting cache key, and the runtime-filter
    // id value in `ActionsDAG::serialize`.
    bool for_cache_key = false;
};

struct SerializedSetsRegistry;

/// Deserialization context passed to `IQueryPlanStep::deserialize`.
struct IQueryPlanStep::Deserialization
{
    ReadBuffer & in;
    DeserializedSetsRegistry & registry;
    std::vector<StoragePtr> storage_holders;    /// Storages that are referenced by the step and need to be kept alive

    const ContextPtr & context;

    const SharedHeaders & input_headers;
    const SharedHeader & output_header;
    const QueryPlanSerializationSettings & settings;
};

}
