#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/Task/DeriveStatGroup.h>
#include <QueryCoordination/Optimizer/Task/EnforceCostGroup.h>
#include <QueryCoordination/Optimizer/Task/OptimizeGroup.h>
#include <QueryCoordination/Optimizer/Task/TransformGroup.h>

namespace DB
{

void OptimizeGroup::execute()
{
    /// push task EnforceAndCost
    auto cost_group = std::make_unique<EnforceCostGroup>(memo, stack, context, group, required_prop);
    pushTask(std::move(cost_group));

    auto derive_stat_group = std::make_unique<DeriveStatGroup>(memo, stack, context, group);
    pushTask(std::move(derive_stat_group));

    /// push task TransformGroup
    auto transform_group = std::make_unique<TransformGroup>(memo, stack, context, group);
    pushTask(std::move(transform_group));
}

}
