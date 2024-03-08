#pragma once

#include <Optimizer/Cost/CostSettings.h>
#include <Optimizer/DeriveRequiredChildProp.h>
#include <Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class GroupNode;
using GroupNodePtr = std::shared_ptr<GroupNode>;

class OptimizeInputs final : public OptimizeTask
{
public:
    /// Support task recovery
    struct Frame
    {
        Frame(GroupNodePtr node, ContextPtr context)
        {
            DeriveRequiredChildProp visitor(node, context);
            alternative_child_prop = node->accept(visitor);

            prop_idx = 0;
            child_idx = 0;
            pre_child_idx = -1;

            local_cost = Cost(CostSettings::fromContext(context).getCostWeight(), 0);
            total_cost = Cost(CostSettings::fromContext(context).getCostWeight(), 0);
        }

        /// Whether it is turn to visit new alternative sub problem.
        bool newAlternativeCalc() const { return pre_child_idx == -1 && child_idx == 0; }

        void resetAlternativeState()
        {
            pre_child_idx = -1;
            child_idx = 0;
            local_cost.reset();
            total_cost.reset();
            actual_children_prop.clear();
        }

        /// Alternative children properties, actually they are child problems, for example as to join,
        /// there are 2 child problems broadcast join and shuffle join.
        /// Note that for leaf node of query plan, alternative_child_prop has one element which has no child property.
        AlternativeChildrenProp alternative_child_prop;
        Int32 prop_idx;

        /// alternative calc frame
        Int32 pre_child_idx;
        Int32 child_idx;
        Cost local_cost;
        Cost total_cost;

        std::vector<PhysicalProperties> actual_children_prop;
    };

    OptimizeInputs(GroupNodePtr group_node_, TaskContextPtr task_context_, std::unique_ptr<Frame> frame_ = nullptr);

    void execute() override;

    String getDescription() override;

private:
    OptimizeTaskPtr clone();

    bool isInitialTask() const;

    Cost enforceGroupNode(const PhysicalProperties & required_prop, const PhysicalProperties & output_prop);

    void enforceTwoLevelAggIfNeed(const PhysicalProperties & required_prop);

    GroupNodePtr group_node;
    std::unique_ptr<Frame> frame;

    Poco::Logger * log;
};

}
