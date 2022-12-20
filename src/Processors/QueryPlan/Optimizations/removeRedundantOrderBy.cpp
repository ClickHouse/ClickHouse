#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/FullSortingMergeJoin.h>
// #include <Planner/Utils.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{
template <typename Derived, bool debug_logging = false>
class QueryPlanVisitor
{
protected:
    struct FrameWithParent
    {
        QueryPlan::Node * node = nullptr;
        QueryPlan::Node * parent_node = nullptr;
        size_t next_child = 0;
    };

    using StackWithParent = std::vector<FrameWithParent>;

    QueryPlan::Node * root = nullptr;
    StackWithParent stack;

public:
    explicit QueryPlanVisitor(QueryPlan::Node * root_) : root(root_) { }

    void visit()
    {
        stack.push_back({.node = root});

        while (!stack.empty())
        {
            auto & frame = stack.back();

            QueryPlan::Node * current_node = frame.node;
            QueryPlan::Node * parent_node = frame.parent_node;

            logStep("back", current_node);

            /// top-down visit
            if (0 == frame.next_child)
            {
                logStep("top-down", current_node);
                if (!visitTopDown(current_node, parent_node))
                    continue;
            }
            /// Traverse all children
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = FrameWithParent{.node = current_node->children[frame.next_child], .parent_node = current_node};
                ++frame.next_child;
                logStep("push", next_frame.node);
                stack.push_back(next_frame);
                continue;
            }

            /// bottom-up visit
            logStep("bottom-up", current_node);
            visitBottomUp(current_node, parent_node);

            logStep("pop", current_node);
            stack.pop_back();
        }
    }

    bool visitTopDown(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        return getDerived().visitTopDown(current_node, parent_node);
    }
    void visitBottomUp(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        getDerived().visitBottomUp(current_node, parent_node);
    }

private:
    Derived & getDerived() { return *static_cast<Derived *>(this); }

    const Derived & getDerived() const { return *static_cast<Derived *>(this); }

protected:
    void logStep(const char * prefix, const QueryPlan::Node * node)
    {
        if constexpr (debug_logging)
        {
            IQueryPlanStep * current_step = node->step.get();
            LOG_DEBUG(
                &Poco::Logger::get("QueryPlanVisitor"),
                "{}: {}: {}",
                prefix,
                current_step->getName(),
                reinterpret_cast<void *>(current_step));
        }
    }
};

constexpr bool debug_logging_enabled = true;

class IsQueryDistributed : public QueryPlanVisitor<IsQueryDistributed, debug_logging_enabled>
{
public:
    explicit IsQueryDistributed(QueryPlan::Node * root_) : QueryPlanVisitor<IsQueryDistributed, debug_logging_enabled>(root_) { }

    explicit operator bool() const { return distributed_query; }

    static bool visitTopDown(QueryPlan::Node *, QueryPlan::Node *) { return true; }

    void visitBottomUp(QueryPlan::Node * current_node, QueryPlan::Node *)
    {
        if (typeid_cast<ReadFromRemote *>(current_node->step.get()))
            distributed_query = true;
    }

private:
    bool distributed_query = false;
};

class RemoveRedundantOrderBy : public QueryPlanVisitor<RemoveRedundantOrderBy, debug_logging_enabled>
{
    std::vector<QueryPlan::Node *> nodes_affect_order;

public:
    explicit RemoveRedundantOrderBy(QueryPlan::Node * root_) : QueryPlanVisitor<RemoveRedundantOrderBy, debug_logging_enabled>(root_) { }

    bool visitTopDown(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        IQueryPlanStep * current_step = current_node->step.get();

        /// if there is parent node which can affect order and current step is sorting
        /// then check if we can remove the sorting step (and corresponding expression step)
        if (!nodes_affect_order.empty() && typeid_cast<SortingStep *>(current_step))
        {
            if (tryRemoveSorting(current_node, parent_node))
            {
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
            || typeid_cast<FillingStep *>(current_step) /// (2) if ORDER BY is with FILL WITH, it is non-removable
            || typeid_cast<SortingStep *>(current_step) /// (3) ORDER BY will change order of previous sorting
            || typeid_cast<AggregatingStep *>(current_step)) /// (4) aggregation change order
        {
            logStep("steps_affect_order/push", current_node);
            nodes_affect_order.push_back(current_node);
        }

        return true;
    }

    void visitBottomUp(QueryPlan::Node * current_node, QueryPlan::Node *)
    {
        /// we come here when all children of current_node are visited,
        /// so it's a node which affect order, remove it from the corresponding stack
        if (!nodes_affect_order.empty() && nodes_affect_order.back() == current_node)
        {
            logStep("node_affect_order/pop", current_node);
            nodes_affect_order.pop_back();
        }
    }

private:
    bool tryRemoveSorting(QueryPlan::Node * sorting_node, QueryPlan::Node * parent_node)
    {
        if (!canRemoveCurrentSorting())
            return false;

        chassert(typeid_cast<ExpressionStep *>(sorting_node->children.front()->step.get()));
        chassert(!sorting_node->children.front()->children.empty());

        /// need to remove sorting
        parent_node->children.front() = sorting_node->children.front();

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
            auto const & aggregates = parent_aggr->getParams().aggregates;
            for (const auto & aggregate : aggregates)
            {
                auto aggregate_function_properties = AggregateFunctionFactory::instance().tryGetProperties(aggregate.function->getName());
                if (aggregate_function_properties && aggregate_function_properties->is_order_dependent)
                    return false;
            }
            return true;
        }
        /// (2) sorting
        else if (typeid_cast<SortingStep *>(step_affect_order))
            return true;

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
            logStep("checking for stateful function", node);

            /// walking though stack until reach node which affects order
            if (node == node_affect_order)
                break;

            const auto * step = node->step.get();

            const auto * expr = typeid_cast<const ExpressionStep *>(step);
            if (expr)
            {
                if (expr->getExpression()->hasStatefulFunctions())
                    return false;
            }
            else
            {
                const auto * trans = typeid_cast<const ITransformingStep *>(step);
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
            logStep("checking path from current sorting", node);

            /// walking though stack until reach node which affects order
            if (node == node_affect_order)
                break;

            const auto * step = node->step.get();

            /// (2) for window function we do ORDER BY in 2 Sorting steps,
            /// so do not delete Sorting if window function step is on top
            if (typeid_cast<const WindowStep *>(step))
                return false;

            /// (3) TODO: need to understand that plan is part of distributed one
            /// and don't try to optimize it since it's already optimized, AFAIU
            /// for now, rough check for disributed query via Union step check
            /// example in 02315_optimize_monotonous_functions_in_order_by_remote.sql
            if (typeid_cast<const UnionStep *>(step))
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

void tryRemoveRedundantOrderBy(QueryPlan::Node * root)
{
    IsQueryDistributed is_distributed(root);
    is_distributed.visit();
    if (is_distributed.operator bool())
        return;

    RemoveRedundantOrderBy(root).visit();
}

}
