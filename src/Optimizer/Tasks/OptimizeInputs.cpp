#include <Optimizer/Cost/CostCalculator.h>
#include <Optimizer/DeriveOutputProp.h>
#include <Optimizer/Group.h>
#include <Optimizer/GroupNode.h>
#include <Optimizer/Memo.h>
#include <Optimizer/Tasks/OptimizeGroup.h>
#include <Optimizer/Tasks/OptimizeInputs.h>
#include <Optimizer/Tasks/OptimizeTask.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Common/typeid_cast.h>


namespace DB
{

OptimizeInputs::OptimizeInputs(GroupNodePtr group_node_, TaskContextPtr task_context_, std::unique_ptr<Frame> frame_)
    : OptimizeTask(task_context_), group_node(group_node_), frame(std::move(frame_))
{
    auto & group = task_context->getCurrentGroup();
    log = &Poco::Logger::get(fmt::format("OptimizeInputs #{}#{} ({})", group.getId(), group_node->getId(), group_node->getStep()->getName()));
}

void OptimizeInputs::execute()
{
    if (group_node->isEnforceNode())
    {
        LOG_TRACE(log, "Skip enforce node");
        return;
    }

    auto & group = task_context->getCurrentGroup();
    LOG_TRACE(log, "Upper bound cost of current group is {}", task_context->getUpperBoundCost().toString());

    const auto & required_prop = task_context->getRequiredProp();
    LOG_TRACE(log, "Required properties is {}", required_prop.distribution.toString());

    std::vector<Stats> children_statistics;
    for (auto & child_group : group_node->getChildren())
        children_statistics.emplace_back(child_group->getStatistics());

    /// If frame is null, means it is the first time optimize node.
    if (!frame)
        frame = std::make_unique<Frame>(group_node, task_context->getQueryContext());

    /// Each alternative children properties is actually a child problem.
    for (; frame->prop_idx < static_cast<Int32>(frame->alternative_child_prop.size()); ++frame->prop_idx)
    {
        LOG_TRACE(log, "Evaluate alternative children properties {}", required_prop.distribution.toString());
        auto & required_child_props = frame->alternative_child_prop[frame->prop_idx];

        if (frame->newAlternativeCalc())
        {
            CostCalculator cost_calc(group.getStatistics(), task_context, children_statistics, required_child_props);
            frame->local_cost = group_node->accept(cost_calc);
            frame->total_cost = frame->local_cost;
            LOG_TRACE(log, "local cost: {}", frame->local_cost.toString());
        }

        const auto & child_groups = group_node->getChildren();
        for (; frame->child_idx < static_cast<Int32>(child_groups.size()); ++frame->child_idx)
        {
            auto required_child_prop = required_child_props[frame->child_idx];
            auto & child_group = *child_groups[frame->child_idx];
            auto best_node = child_group.getSatisfiedBestGroupNode(required_child_prop);
            if (!best_node)
            {
                if (frame->pre_child_idx >= frame->child_idx) /// child problem no solution
                    break;

                frame->pre_child_idx = frame->child_idx;
                auto child_upper_bound_cost = task_context->getUpperBoundCost() - frame->total_cost;

                if (child_upper_bound_cost.get() <= 0.0)
                    LOG_INFO(
                        log,
                        "upper bound cost of child group {} is {}, which is lower than 0, which means it will have no solution",
                        child_group.getId(),
                        child_upper_bound_cost.get());

                /// Push current task frame to the task stack.
                pushTask(clone());

                /// Child group is not optimized, push child task to the task stack.
                TaskContextPtr child_task_context = std::make_shared<TaskContext>(
                    child_group, required_child_prop, task_context->getOptimizeContext(), child_upper_bound_cost);
                pushTask(std::make_unique<OptimizeGroup>(child_task_context));
                return;
            }
            frame->total_cost += best_node->second.cost;

            if (frame->total_cost >= task_context->getUpperBoundCost()) /// this problem has no solution
                break;

            frame->actual_children_prop.emplace_back(best_node->first);
        }

        /// All child problem has solution
        if (frame->child_idx == static_cast<Int32>(group_node->getChildren().size()))
        {
            /// derive output prop by required_prop and children_prop
            DeriveOutputProp output_prop_visitor(group_node, required_prop, frame->actual_children_prop, task_context->getQueryContext());
            auto output_prop = group_node->accept(output_prop_visitor);

            auto child_cost = frame->total_cost - frame->local_cost;

            if (group_node->updateBestChild(output_prop, frame->actual_children_prop, child_cost))
            {
                String children_prop_desc = "[";
                for (auto & actual_child_prop : frame->actual_children_prop)
                    children_prop_desc += actual_child_prop.toString() + ", ";
                if (frame->actual_children_prop.empty())
                    children_prop_desc += "]";
                else
                    children_prop_desc[children_prop_desc.size() - 1] = ']';
                LOG_DEBUG(
                    log,
                    "Group node update best for property {} to {}, children cost: {}",
                    output_prop.toString(),
                    children_prop_desc,
                    child_cost.toString());
            }

            if (group.updatePropBestNode(output_prop, group_node, frame->total_cost))
            {
                LOG_DEBUG(
                    log,
                    "Group update best for property {} to {}, total cost: {}",
                    output_prop.toString(),
                    group_node->getDescription(),
                    frame->total_cost.toString());
            }

            /// Currently, it only deals with distributed cases
            if (!output_prop.satisfySorting(required_prop))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Sort property not satisfied, output sort prop {}, required sort prop {}",
                    output_prop.sort_prop.toString(),
                    required_prop.sort_prop.toString());

            if (!output_prop.satisfyDistribution(required_prop))
            {
                enforceTwoLevelAggIfNeed(required_prop);
                frame->total_cost = enforceGroupNode(required_prop, output_prop);
            }

            if (frame->total_cost < task_context->getUpperBoundCost())
                task_context->setUpperBoundCost(frame->total_cost);
        }

        frame->resetAlternativeState();
    }
}


void OptimizeInputs::enforceTwoLevelAggIfNeed(const PhysicalProperties & required_prop)
{
    if (!(required_prop.distribution.type == Distribution::Type::Hashed && required_prop.distribution.distributed_by_bucket_num))
        return;

    auto * aggregate_step = typeid_cast<AggregatingStep *>(group_node->getStep().get());
    if (!aggregate_step || !aggregate_step->isPreliminaryAgg())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Required distribution by bucket num, but is not preliminary AggregatingStep");

    /// Distribution type is Hashed and use distributed_by_bucket_num need enforce two level aggregate.
    /// Prevent different nodes from using different aggregation policies which causes the data to be incorrect.
    LOG_TRACE(log, "Enforce two level aggregate");
    aggregate_step->enforceTwoLevelAgg();
}

Cost OptimizeInputs::enforceGroupNode(const PhysicalProperties & required_prop, const PhysicalProperties & output_prop)
{
    std::shared_ptr<ExchangeDataStep> exchange_step;

    size_t max_block_size = task_context->getQueryContext()->getSettings().max_block_size;
    /// Because the ordering of data may be changed after adding Exchange in a distributed manner, we need to retain the order of data during exchange if there is a requirement for data sorting.
    if (required_prop.sort_prop.sort_scope == DataStream::SortScope::Stream
        && output_prop.sort_prop.sort_scope >= DataStream::SortScope::Stream)
    {
        exchange_step = std::make_shared<ExchangeDataStep>(
            required_prop.distribution,
            group_node->getStep()->getOutputStream(),
            max_block_size,
            required_prop.sort_prop.sort_description,
            DataStream::SortScope::Stream,
            true);
    }
    else if (required_prop.sort_prop.sort_scope == DataStream::SortScope::Global)
    {
        exchange_step = std::make_shared<ExchangeDataStep>(
            required_prop.distribution,
            group_node->getStep()->getOutputStream(),
            max_block_size,
            required_prop.sort_prop.sort_description,
            DataStream::SortScope::Global,
            true,
            true);
    }
    else
    {
        exchange_step
            = std::make_shared<ExchangeDataStep>(required_prop.distribution, group_node->getStep()->getOutputStream(), max_block_size);
    }

    auto & group = task_context->getCurrentGroup();

    std::vector<Group *> children;
    GroupNodePtr group_enforce_node = std::make_shared<GroupNode>(exchange_step, children, true);

    auto group_node_id = task_context->getMemo().fetchAddGroupNodeId();
    group.addGroupNode(group_enforce_node, group_node_id);

    auto child_cost = group.getCostByProp(output_prop);

    CostCalculator cost_calc(group.getStatistics(), task_context);
    auto cost = group_enforce_node->accept(cost_calc);
    Cost total_cost = cost + child_cost;
    LOG_TRACE(log, "Enforcing ExchangeData with cost {} and now total cost is {}", cost.toString(), total_cost.toString());

    DeriveOutputProp output_prop_visitor(group_enforce_node, required_prop, {output_prop}, task_context->getQueryContext());

    const auto & actual_output_prop = group_enforce_node->accept(output_prop_visitor);

    group_enforce_node->updateBestChild(actual_output_prop, {output_prop}, child_cost);
    group.updatePropBestNode(actual_output_prop, group_enforce_node->shared_from_this(), total_cost);

    return total_cost;
}

OptimizeTaskPtr OptimizeInputs::clone()
{
    return std::make_unique<OptimizeInputs>(group_node, task_context, std::move(frame));
}

String OptimizeInputs::getDescription()
{
    return "OptimizeInputs (" + group_node->getStep()->getName() + ")";
}

}
