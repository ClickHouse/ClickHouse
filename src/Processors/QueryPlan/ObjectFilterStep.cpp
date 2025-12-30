#include <Processors/QueryPlan/ObjectFilterStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
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

void ObjectFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);

    actions_dag.serialize(ctx.out, ctx.registry);
}

std::unique_ptr<IQueryPlanStep> ObjectFilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ObjectFilterStep must have one input stream");

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);

    return std::make_unique<ObjectFilterStep>(ctx.input_headers.front(), std::move(actions_dag), std::move(filter_column_name));
}

void registerObjectFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ObjectFilter", ObjectFilterStep::deserialize);
}

}
