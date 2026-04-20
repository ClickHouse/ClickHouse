#include <Processors/QueryPlan/OptimizationBarrierStep.h>

#include <Core/Block.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
    constexpr ITransformingStep::Traits getTraits()
    {
        return ITransformingStep::Traits
        {
            {
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = true,
            }
        };
    }

    String formatAllowedOptimizations(const OptimizationBarrierStep::AllowedOptimizations & a)
    {
        WriteBufferFromOwnString out;
        bool first = true;
        auto append = [&](const char * name, bool value)
        {
            if (!value)
                return;
            if (!first)
                out << ", ";
            out << name;
            first = false;
        };
        append("push_down_limit", a.push_down_limit);
        append("remove_unused_columns", a.remove_unused_columns);
        append("read_in_order", a.read_in_order);
        if (first)
            out << "(none)";
        return out.str();
    }
}

OptimizationBarrierStep::OptimizationBarrierStep(
    const SharedHeader & header,
    AllowedOptimizations allowed_optimizations_)
    : ITransformingStep(header, header, getTraits())
    , allowed_optimizations(allowed_optimizations_)
{
}

void OptimizationBarrierStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Allowed optimizations: " << formatAllowedOptimizations(allowed_optimizations) << '\n';
}

void OptimizationBarrierStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("AllowedOptimizations", formatAllowedOptimizations(allowed_optimizations));
}

void OptimizationBarrierStep::serialize(Serialization & ctx) const
{
    writeBinary(allowed_optimizations.push_down_limit, ctx.out);
    writeBinary(allowed_optimizations.remove_unused_columns, ctx.out);
    writeBinary(allowed_optimizations.read_in_order, ctx.out);
}

QueryPlanStepPtr OptimizationBarrierStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "OptimizationBarrierStep must have one input stream");

    AllowedOptimizations a;
    readBinary(a.push_down_limit, ctx.in);
    readBinary(a.remove_unused_columns, ctx.in);
    readBinary(a.read_in_order, ctx.in);

    return std::make_unique<OptimizationBarrierStep>(ctx.input_headers.front(), a);
}

void registerOptimizationBarrierStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("OptimizationBarrier", OptimizationBarrierStep::deserialize);
}

IQueryPlanStep::RemovedUnusedColumns OptimizationBarrierStep::removeUnusedColumns(NameMultiSet required_outputs, bool remove_inputs)
{
    /// Pass-through step: input and output must stay identical, so shrink only when opted in
    /// and the optimizer also allows reducing the input side.
    if (!allow_remove_unused_columns() || !remove_inputs)
        return RemovedUnusedColumns::None;

    const auto & old_header = *output_header;
    Block new_header;
    for (const auto & col : old_header)
    {
        if (required_outputs.contains(col.name))
            new_header.insert(col);
    }

    if (new_header.columns() == old_header.columns())
        return RemovedUnusedColumns::None;

    auto new_shared_header = std::make_shared<const Block>(std::move(new_header));
    updateInputHeader(new_shared_header, 0); /// also refreshes output_header via updateOutputHeader()
    return RemovedUnusedColumns::OutputAndInput;
}

}
