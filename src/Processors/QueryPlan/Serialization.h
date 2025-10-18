#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
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
};

struct SerializedSetsRegistry;

/// Deserialization context passed to `IQueryPlanStep::deserialize`.
struct IQueryPlanStep::Deserialization
{
    ReadBuffer & in;
    DeserializedSetsRegistry & registry;
    const ContextPtr & context;

    const SharedHeaders & input_headers;
    const SharedHeader & output_header;
    const QueryPlanSerializationSettings & settings;
};

}
