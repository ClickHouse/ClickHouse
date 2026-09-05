#include <Processors/QueryPlan/ObjectFilterStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/Transforms/FilterTransform.h>
#include <IO/Operators.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

ObjectFilterStep::ObjectFilterStep(
    const SharedHeader & input_header_,
    ActionsDAG actions_dag_,
    String filter_column_name_)
    : actions_dag(std::move(actions_dag_))
    , filter_column_name(std::move(filter_column_name_))
{
    input_headers.emplace_back(input_header_);
    output_header = input_headers.front();
}

QueryPipelineBuilderPtr ObjectFilterStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /* settings */)
{
    return std::move(pipelines.front());
}

void ObjectFilterStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

namespace
{

constexpr auto OBJECT_FILTER_MANIFEST = StepManifest<ObjectFilterStep, ObjectFilterWire>("ObjectFilter")
    .nameIntroducedIn(1)
    .inputs(1)
    .baseFormat(
        field("actions_dag", WireFieldClass::Logical, &ObjectFilterWire::actions_dag),
        field("filter_column_name", WireFieldClass::Logical, &ObjectFilterWire::filter_column_name));

}

ObjectFilterWire ObjectFilterStep::toWire() const
{
    return ObjectFilterWire{actions_dag.clone(), filter_column_name};
}

QueryPlanStepPtr ObjectFilterStep::fromWire(ObjectFilterWire wire, Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ObjectFilterStep must have one input stream");

    return std::make_unique<ObjectFilterStep>(ctx.input_headers.front(), std::move(wire.actions_dag), std::move(wire.filter_column_name));
}

void ObjectFilterStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(OBJECT_FILTER_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr ObjectFilterStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(OBJECT_FILTER_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void ObjectFilterStep::serializeLegacy(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);

    actions_dag.serialize(ctx.out, ctx.registry);
}

QueryPlanStepPtr ObjectFilterStep::deserializeLegacy(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ObjectFilterStep must have one input stream");

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity, bytesRemainingInFrame(ctx.in));

    return std::make_unique<ObjectFilterStep>(ctx.input_headers.front(), std::move(actions_dag), std::move(filter_column_name));
}

void registerObjectFilterStep(QueryPlanStepRegistry & registry);
void registerObjectFilterStep(QueryPlanStepRegistry & registry)
{
    registerManifest<OBJECT_FILTER_MANIFEST>(registry, ObjectFilterStep::deserialize);
}

}
