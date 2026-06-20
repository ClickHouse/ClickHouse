#pragma once

#include <string>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

/// Serializes ClickHouse QueryPlan to Substrait Plan protobuf
class SubstraitSerializer
{
public:
    explicit SubstraitSerializer() = default;

    /// Convert a QueryPlan to Substrait Plan
    /// Returns the plan as a binary protobuf serialized string
    std::string serializePlanToBinary(const QueryPlan & query_plan);

    /// Convert a QueryPlan to Substrait Plan
    /// Returns the plan as a JSON string (for debugging/display)
    std::string serializePlanToJSON(const QueryPlan & query_plan);

private:
    /// Main conversion method
    class Impl;
};

}
