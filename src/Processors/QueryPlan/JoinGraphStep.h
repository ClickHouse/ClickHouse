#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <memory>
#include <vector>

namespace DB
{

/// A join graph that the join order optimizer has solved and the Cascades optimizer will order. Its children are the
/// graph's inputs, each present once. The plan of the chosen join order is built already and kept inside the step,
/// out of the plan: it gives the step its header and its estimate, and it replaces the step where Cascades does not
/// run (`expandToChosenOrder`). The plans of the other orders the optimizer found are built only when Cascades imports
/// the graph, over the same inputs, so that Cascades imports every input once.
class JoinGraphStep : public IQueryPlanStep
{
public:
    /// Builds the plans of the other join orders over the step's inputs.
    class OrderBuilder
    {
    public:
        virtual ~OrderBuilder() = default;
        virtual size_t otherOrdersCount() const = 0;
        /// Builds other order `index` in `nodes` and returns its root. The orders share the plans of their common
        /// subtrees: a subtree some order has already is not built again.
        virtual QueryPlan::Node * buildOtherOrder(size_t index, const std::vector<QueryPlan::Node *> & inputs, QueryPlan::Nodes & nodes) = 0;
    };

    /// `chosen_order_root` is the root of the chosen order's plan; its leaves are the nodes in `inputs` order.
    JoinGraphStep(SharedHeaders input_headers_, QueryPlan::Node * chosen_order_root_, std::shared_ptr<OrderBuilder> order_builder_);

    String getName() const override { return "JoinGraph"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

    /// The root of the chosen order's plan, whose leaves are the step's children.
    QueryPlan::Node * getChosenOrderRoot() const { return chosen_order_root; }
    OrderBuilder & getOrderBuilder() const { return *order_builder; }

    /// Replaces the step in `node` by the plan of the chosen order.
    static void expandToChosenOrder(QueryPlan::Node & node);

private:
    void updateOutputHeader() override;

    QueryPlan::Node * chosen_order_root;
    std::shared_ptr<OrderBuilder> order_builder;
    SharedHeaders original_input_headers;
};

}
