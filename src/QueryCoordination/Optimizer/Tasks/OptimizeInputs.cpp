#include <QueryCoordination/Optimizer/Cost/CostCalculator.h>
#include <QueryCoordination/Optimizer/DeriveOutputProp.h>
#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeGroup.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeInputs.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>


namespace DB
{

OptimizeInputs::OptimizeInputs(GroupNodePtr group_node_, TaskContextPtr task_context_, std::unique_ptr<Frame> frame_)
    : OptimizeTask(task_context_), group_node(group_node_), frame(std::move(frame_))
{
    auto & group = task_context->getCurrentGroup();
    log = &Poco::Logger::get("OptimizeInputs group(" + std::to_string(group.getId()) + ") group node(" + group_node->getStep()->getName() + ")");
}

void OptimizeInputs::execute()
{
    if (group_node->isEnforceNode())
        return;

    auto & group = task_context->getCurrentGroup();
    const auto & required_prop = task_context->getRequiredProp();

    std::vector<Statistics> children_statistics;
    for (auto & child_group : group_node->getChildren())
        children_statistics.emplace_back(child_group->getStatistics());

    if (!frame)
        frame = std::make_unique<Frame>(group_node);

    LOG_TRACE(log, "It's upper bound cost: {}", task_context->getUpperBoundCost().toString());

    /// every alternative prop, required to child
    for (; frame->prop_idx < static_cast<Int32>(frame->alternative_child_prop.size()); ++frame->prop_idx)
    {
        auto & required_child_props = frame->alternative_child_prop[frame->prop_idx];

        if (frame->newAlternativeCalc())
        {
            CostCalculator cost_calc(group.getStatistics(), task_context->getQueryContext(), children_statistics, required_child_props);
            frame->local_cost = group_node->accept(cost_calc);

            LOG_TRACE(log, "It's local cost: {}", frame->local_cost.toString());

            frame->total_cost = frame->local_cost;
        }

        const auto & child_groups = group_node->getChildren();
        for (; frame->child_idx < static_cast<Int32>(child_groups.size()); ++frame->child_idx)
        {
            auto required_child_prop = required_child_props[frame->child_idx];
            auto & child_group = *child_groups[frame->child_idx];
            auto best_node = child_group.getSatisfyBestGroupNode(required_child_prop);
            if (!best_node)
            {
                if (frame->pre_child_idx >= frame->child_idx) /// child problem no solution
                    break;

                frame->pre_child_idx = frame->child_idx;
                auto child_upper_bound_cost = task_context->getUpperBoundCost() - frame->total_cost;

                pushTask(clone());

                TaskContextPtr child_task_context = std::make_shared<TaskContext>(
                    child_group, required_child_prop, task_context->getOptimizeContext(), child_upper_bound_cost);
                pushTask(std::make_unique<OptimizeGroup>(child_task_context));
                return;
            }
            frame->total_cost += best_node->second.cost;

            if (frame->total_cost >= task_context->getUpperBoundCost()) /// this task no solution
                break;

            frame->actual_children_prop.emplace_back(best_node->first);
        }

        /// all child problem has solution
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
                    "GroupNode {} update best for property {} to {}, cost: {}",
                    group_node->getDescription(),
                    output_prop.toString(),
                    children_prop_desc,
                    child_cost.toString());
            }

            if (group.updatePropBestNode(output_prop, group_node, frame->total_cost))
            {
                LOG_DEBUG(
                    log,
                    "{} update best for property {} to {}, total cost: {}",
                    group.getDescription(),
                    output_prop.toString(),
                    group_node->getDescription(),
                    frame->total_cost.toString());
            }

            /// Currently, it only deals with distributed cases
            if (!output_prop.satisfySort(required_prop))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Sort property not satisfied, output sort prop {}, required sort prop {}", output_prop.sort_prop.toString(), required_prop.sort_prop.toString());

            if (!output_prop.satisfyDistribute(required_prop))
            {
                frame->total_cost = enforceGroupNode(required_prop, output_prop);
            }

            if (frame->total_cost < task_context->getUpperBoundCost())
            {
                task_context->setUpperBoundCost(frame->total_cost);
            }
        }

        frame->resetAlternativeState();
    }
}


Cost OptimizeInputs::enforceGroupNode(
    const PhysicalProperties & required_prop,
    const PhysicalProperties & output_prop)
{
    std::shared_ptr<ExchangeDataStep> exchange_step;

    size_t max_block_size = task_context->getQueryContext()->getSettings().max_block_size;
    /// Because the ordering of data may be changed after adding Exchange in a distributed manner, we need to retain the order of data during exchange if there is a requirement for data sorting.
    if (required_prop.sort_prop.sort_scope == DataStream::SortScope::Stream && output_prop.sort_prop.sort_scope >= DataStream::SortScope::Stream)
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
        exchange_step = std::make_shared<ExchangeDataStep>(required_prop.distribution, group_node->getStep()->getOutputStream(), max_block_size);
    }

    auto & group = task_context->getCurrentGroup();

    std::vector<Group *> children;
    GroupNodePtr group_enforce_node = std::make_shared<GroupNode>(exchange_step, children, true);

    auto group_node_id = task_context->getMemo().fetchAddGroupNodeId();
    group.addGroupNode(group_enforce_node, group_node_id);

    auto child_cost = group.getCostByProp(output_prop);

    CostCalculator cost_calc(group.getStatistics(), task_context->getQueryContext());
    Cost total_cost = group_enforce_node->accept(cost_calc) + child_cost;

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
