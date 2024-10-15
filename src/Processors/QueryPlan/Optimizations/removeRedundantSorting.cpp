#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{
constexpr bool debug_logging_enabled = false;

class RemoveRedundantSorting : public QueryPlanVisitor<RemoveRedundantSorting, debug_logging_enabled>
{
    /// stack with nodes which affect order
    /// nodes added when traversing top-down
    /// as soon as all children for the node on top of stack are traversed, the node is removed from stack
    std::vector<QueryPlan::Node *> nodes_affect_order;

public:
    explicit RemoveRedundantSorting(QueryPlan::Node * root_) : QueryPlanVisitor<RemoveRedundantSorting, debug_logging_enabled>(root_) { }

    bool visitTopDownImpl(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        IQueryPlanStep * current_step = current_node->step.get();

        /// if there is parent node which can affect order and current step is sorting
        /// then check if we can remove the sorting step (and corresponding expression step)
        if (!nodes_affect_order.empty() && typeid_cast<SortingStep *>(current_step))
        {
            if (tryRemoveSorting(current_node, parent_node))
            {
                logStep("step affect sorting", nodes_affect_order.back());
                logStep("removed from plan", current_node);

                auto & frame = stack.back();
                /// mark removed node as visited
                frame.next_child = frame.node->children.size();

                /// current sorting step has been removed from plan, its parent has new children, need to visit them
                auto next_frame = FrameWithParent{.node = parent_node->children[0], .parent_node = parent_node};
                stack.push_back(next_frame);
                logStep("push", next_frame.node);
                return false;
            }
        }

        if (typeid_cast<LimitStep *>(current_step)
            || typeid_cast<LimitByStep *>(current_step) /// (1) if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
            || typeid_cast<OffsetStep *>(current_step) /// (2) OFFSET on top of ORDER BY, the ORDER BY is non-removable
            || typeid_cast<FillingStep *>(current_step) /// (3) if ORDER BY is with FILL WITH, it is non-removable
            || typeid_cast<SortingStep *>(current_step) /// (4) ORDER BY will change order of previous sorting
            || typeid_cast<AggregatingStep *>(current_step)) /// (5) aggregation change order
        {
            logStep("nodes_affect_order/push", current_node);
            nodes_affect_order.push_back(current_node);
        }

        return true;
    }

    void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node *)
    {
        /// we come here when all children of current_node are visited,
        /// so, if it's a node which affect order, remove it from the corresponding stack
        if (!nodes_affect_order.empty() && nodes_affect_order.back() == current_node)
        {
            logStep("nodes_affect_order/pop", current_node);
            nodes_affect_order.pop_back();
        }
    }

private:
    bool tryRemoveSorting(QueryPlan::Node * sorting_node, QueryPlan::Node * parent_node)
    {
        if (!canRemoveCurrentSorting())
            return false;

        /// remove sorting
        for (auto & child : parent_node->children)
        {
            if (child == sorting_node)
            {
                child = sorting_node->children.front();
                break;
            }
        }

        /// sorting removed, so need to update sorting traits for upstream steps
        const Header * input_header = &parent_node->children.front()->step->getOutputHeader();
        chassert(parent_node == (stack.rbegin() + 1)->node); /// skip element on top of stack since it's sorting which was just removed
        for (StackWithParent::const_reverse_iterator it = stack.rbegin() + 1; it != stack.rend(); ++it)
        {
            const QueryPlan::Node * node = it->node;
            /// skip removed sorting steps
            auto * step = node->step.get();
            if (typeid_cast<const SortingStep *>(step) && node != nodes_affect_order.back())
                continue;

            logStep("update sorting traits", node);

            auto * trans = dynamic_cast<ITransformingStep *>(step);
            if (!trans)
            {
                logStep("stop update sorting traits: node is not transforming step", node);
                break;
            }

            trans->updateInputHeader(*input_header);
            input_header = &trans->getOutputHeader();

            /// update sorting properties though stack until reach node which affects order (inclusive)
            if (node == nodes_affect_order.back())
            {
                logStep("stop update sorting traits: reached node which affect order", node);
                break;
            }
        }

        return true;
    }

    bool canRemoveCurrentSorting()
    {
        chassert(!stack.empty());
        chassert(typeid_cast<const SortingStep *>(stack.back().node->step.get()));

        return checkNodeAffectingOrder(nodes_affect_order.back()) && checkPathFromCurrentSortingNode(nodes_affect_order.back());
    }

    static bool checkNodeAffectingOrder(QueryPlan::Node * node_affect_order)
    {
        IQueryPlanStep * step_affect_order = node_affect_order->step.get();

        /// if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
        /// if ORDER BY is with FILL WITH, it is non-removable
        if (typeid_cast<LimitStep *>(step_affect_order) || typeid_cast<LimitByStep *>(step_affect_order)
            || typeid_cast<FillingStep *>(step_affect_order))
            return false;

        /// (1) aggregation
        if (const AggregatingStep * parent_aggr = typeid_cast<AggregatingStep *>(step_affect_order); parent_aggr)
        {
            if (parent_aggr->inOrder())
                return false;

            auto const & aggregates = parent_aggr->getParams().aggregates;
            for (const auto & aggregate : aggregates)
            {
                auto action = NullsAction::EMPTY;
                auto aggregate_function_properties
                    = AggregateFunctionFactory::instance().tryGetProperties(aggregate.function->getName(), action);
                if (aggregate_function_properties && aggregate_function_properties->is_order_dependent)
                    return false;

                /// sum*() with Floats depends on order
                /// but currently, there is no way to specify property `is_order_dependent` for combination of aggregating function and data type as argument
                /// so, we check explicitly for sum*() functions with Floats here
                const auto aggregate_function = aggregate.function;
                const String & func_name = aggregate_function->getName();
                if (func_name.starts_with("sum"))
                {
                    DataTypePtr data_type = aggregate_function->getArgumentTypes().front();
                    if (WhichDataType(removeNullable(data_type)).isFloat())
                        return false;
                }
            }
            return true;
        }
        /// (2) sorting
        if (const auto * next_sorting = typeid_cast<const SortingStep *>(step_affect_order); next_sorting)
        {
            if (next_sorting->getType() == SortingStep::Type::Full)
                return true;
        }

        return false;
    }

    bool checkPathFromCurrentSortingNode(const QueryPlan::Node * node_affect_order)
    {
        chassert(!stack.empty());
        chassert(typeid_cast<const SortingStep *>(stack.back().node->step.get()));

        /// (1) if there is expression with stateful function between current step
        /// and step which affects order, then we need to keep sorting since
        /// stateful function output can depend on order

        /// skip element on top of stack since it's sorting
        for (StackWithParent::const_reverse_iterator it = stack.rbegin() + 1; it != stack.rend(); ++it)
        {
            const QueryPlan::Node * node = it->node;
            /// walking though stack until reach node which affects order
            if (node == node_affect_order)
                break;

            const auto * step = node->step.get();
            /// skip removed sorting steps
            if (typeid_cast<const SortingStep*>(step))
                continue;

            logStep("checking for stateful function", node);
            if (const auto * expr = typeid_cast<const ExpressionStep *>(step); expr)
            {
                if (expr->getExpression().hasStatefulFunctions())
                    return false;
            }
            else if (const auto * filter = typeid_cast<const FilterStep *>(step); filter)
            {
                if (filter->getExpression().hasStatefulFunctions())
                    return false;
            }
            else
            {
                const auto * trans = dynamic_cast<const ITransformingStep *>(step);
                if (!trans)
                    break;

                if (!trans->getDataStreamTraits().preserves_sorting)
                    break;
            }
        }

        /// check steps on stack if there are some which can prevent from removing SortingStep
        for (StackWithParent::const_reverse_iterator it = stack.rbegin() + 1; it != stack.rend(); ++it)
        {
            const QueryPlan::Node * node = it->node;
            /// walking though stack until reach node which affects order
            if (node == node_affect_order)
                break;

            const auto * step = node->step.get();
            /// skip removed sorting steps
            if (typeid_cast<const SortingStep *>(step))
                continue;

            logStep("checking path from current sorting", node);

            /// (2) for window function we do ORDER BY in 2 Sorting steps,
            /// so do not delete Sorting if window function step is on top
            if (typeid_cast<const WindowStep *>(step))
                return false;

            if (const auto * join_step = typeid_cast<const JoinStep *>(step); join_step)
            {
                if (typeid_cast<const FullSortingMergeJoin *>(join_step->getJoin().get()))
                    return false;
            }
        }

        return true;
    }
};

void tryRemoveRedundantSorting(QueryPlan::Node * root)
{
    RemoveRedundantSorting(root).visit();
}

}
