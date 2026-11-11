#include <Processors/QueryPlan/JoinGraphStep.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

JoinGraphStep::JoinGraphStep(SharedHeaders input_headers_, QueryPlan::Node * chosen_order_root_, std::shared_ptr<OrderBuilder> order_builder_)
    : chosen_order_root(chosen_order_root_)
    , order_builder(std::move(order_builder_))
    , original_input_headers(input_headers_)
{
    updateInputHeaders(std::move(input_headers_));
}

void JoinGraphStep::updateOutputHeader()
{
    /// The plans of the orders are built against the input headers the step was created with.
    if (input_headers.size() != original_input_headers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinGraph expects {} inputs, got {}", original_input_headers.size(), input_headers.size());
    for (size_t i = 0; i < input_headers.size(); ++i)
        if (!blocksHaveEqualStructure(*input_headers[i], *original_input_headers[i]))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinGraph input {} changed its header from {} to {}",
                i, original_input_headers[i]->dumpStructure(), input_headers[i]->dumpStructure());
    output_header = chosen_order_root->step->getOutputHeader();
}

QueryPipelineBuilderPtr JoinGraphStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinGraph must be imported by Cascades or expanded before the pipeline is built");
}

QueryPlanStepPtr JoinGraphStep::clone() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot clone a JoinGraph step");
}

void JoinGraphStep::expandToChosenOrder(QueryPlan::Node & node)
{
    const auto * graph_step = typeid_cast<const JoinGraphStep *>(node.step.get());
    if (!graph_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a JoinGraph step, got {}", node.step->getName());
    auto * root = graph_step->getChosenOrderRoot();
    auto step = std::move(root->step);
    auto children = std::move(root->children);
    auto cost_estimation = root->cost_estimation;
    node.step = std::move(step);
    node.children = std::move(children);
    node.cost_estimation = cost_estimation;
}

}
