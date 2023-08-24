#pragma once

#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>
#include <QueryCoordination/Optimizer/DeriveRequiredChildProp.h>

namespace DB
{

class GroupNode;

class OptimizeInputs final : public OptimizeTask
{
public:
    /// support recover task
    struct Frame
    {
        Frame(GroupNode & node)
        {
            DeriveRequiredChildProp visitor(node);
            alternative_child_prop = node.accept(visitor);
        }

        bool newAlternativeCalc() const
        {
            return pre_child_idx == -1 && child_idx == 0;
        }

        void resetAlternativeState()
        {
            pre_child_idx = -1;
            child_idx = 0;
            local_cost = 0;
            total_cost = 0;
            actual_children_prop.clear();
        }

        AlternativeChildrenProp alternative_child_prop;
        Int32 prop_idx{0};

        /// alternative calc frame
        Int32 pre_child_idx{-1};
        Int32 child_idx{0};
        Float64 local_cost{0};
        Float64 total_cost{0};

        std::vector<PhysicalProperties> actual_children_prop;
    };

    OptimizeInputs(GroupNode & group_node_, TaskContextPtr task_context_, std::unique_ptr<Frame> frame_ = nullptr);

    void execute() override;

    String getDescription() override;

private:
    OptimizeTaskPtr clone();

    bool isInitialTask() const;

    Float64 enforceGroupNode(
        const PhysicalProperties & required_prop,
        const PhysicalProperties & output_prop);

    GroupNode & group_node;
    std::unique_ptr<Frame> frame;
};

}
