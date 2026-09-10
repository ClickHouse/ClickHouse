#pragma once

#include <Interpreters/AggregateDescription.h>
#include <Processors/QueryPlan/StepManifest.h>

/// Codec types that more than one step's wire struct uses and that no core header owns.

namespace DB
{

/// Aggregate descriptions written without their argument columns: what `Cube` and `Rollup` carry,
/// since their aggregates only merge states. A distinct type, because the same C++ type has
/// another encoding elsewhere.
struct AggregateDescriptionsWithoutArguments
{
    AggregateDescriptions value;
};

template <>
struct WireCodec<AggregateDescriptionsWithoutArguments>
{
    static constexpr const char * name = "AggregateDescriptionsWithoutArguments";

    static void write(const AggregateDescriptionsWithoutArguments & aggregates, IQueryPlanStep::Serialization & ctx)
    {
        serializeAggregateDescriptionsWithoutArguments(aggregates.value, ctx.out);
    }

    static void read(AggregateDescriptionsWithoutArguments & aggregates, IQueryPlanStep::Deserialization & ctx)
    {
        deserializeAggregateDescriptionsWithoutArguments(aggregates.value, ctx.in, ctx.max_type_complexity);
    }
};

}
