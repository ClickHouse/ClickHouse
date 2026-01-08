#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct SerializedSetsRegistry;
struct DeserializedSetsRegistry;

struct IQueryPlanStep::Serialization
{
    WriteBuffer & out;
    SerializedSetsRegistry & registry;

    // A durty hack used by the automatic parallel replicas implementation:
    // the `final` value differs for `AggregatingStep` in single-node and distributed query plans.
    // This breaks matching by hash.
    bool skip_final_flag = false;
    // The same situation as above.
    bool skip_cache_key = false;
};

struct SerializedSetsRegistry;

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
