#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Cache-private query plan serialization version.
/// This is intentionally independent from `DBMS_QUERY_PLAN_SERIALIZATION_VERSION`,
/// which is part of the distributed query plan protocol.
static constexpr UInt64 QUERY_PLAN_CACHE_SERIALIZATION_VERSION = 1;

struct SerializedSetsRegistry;
struct DeserializedSetsRegistry;

/// Serialization context passed to `IQueryPlanStep::serialize`.
/// Settings are handled separately via `serializeSettings` method.
struct IQueryPlanStep::Serialization
{
    WriteBuffer & out;
    SerializedSetsRegistry & registry;
    UInt64 version = 0;

    // A durty hack used by the automatic parallel replicas implementation:
    // the `final` value differs for `AggregatingStep` in single-node and distributed query plans.
    // This breaks matching by hash.
    bool skip_final_flag = false;
    // The same situation as above.
    bool skip_cache_key = false;
};

struct SerializedSetsRegistry;

/// Deserialization context passed to `IQueryPlanStep::deserialize`.
struct IQueryPlanStep::Deserialization
{
    ReadBuffer & in;
    DeserializedSetsRegistry & registry;
    UInt64 version = 0;
    std::vector<StoragePtr> storage_holders;    /// Storages that are referenced by the step and need to be kept alive

    const ContextPtr & context;

    const SharedHeaders & input_headers;
    const SharedHeader & output_header;
    const QueryPlanSerializationSettings & settings;
};

}
