#pragma once

#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveRequiredChildPropVisitor : public PlanStepVisitor<AlternativeChildrenProp>
{
public:
    DeriveRequiredChildPropVisitor(GroupNode & group_node_) : group_node(group_node_) {}

    using Base = PlanStepVisitor<AlternativeChildrenProp>;

    AlternativeChildrenProp visit(QueryPlanStepPtr step) override
    {
        if (group_node.hasRequiredChildrenProp())
        {
            return group_node.getRequiredChildrenProp();
        }

        return Base::visit(step);
    }

    AlternativeChildrenProp visitStep() override
    {
        AlternativeChildrenProp res;
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
        return res;
    }

    AlternativeChildrenProp visit(ReadFromMergeTree & /*step*/) override
    {
        //    TODO sort_description by pk, DistributionType by distributed table
        AlternativeChildrenProp res;
        res.emplace_back();
        return res;
    }

    AlternativeChildrenProp visit(AggregatingStep & step) override
    {
        AlternativeChildrenProp res;

        if (step.isFinal())
        {
            std::vector<PhysicalProperties> required_child_prop;
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

            res.emplace_back(required_child_prop);

            std::vector<PhysicalProperties> required_child_prop1;
            required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

            res.emplace_back(required_child_prop1);

            return res;
        }
        else
        {
            std::vector<PhysicalProperties> required_child_prop;
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
            res.emplace_back(required_child_prop);
            return res;
        }
    }

    AlternativeChildrenProp visit(MergingAggregatedStep & step) override
    {
        AlternativeChildrenProp res;
        std::vector<PhysicalProperties> required_child_prop;

        if (step.getParams().keys.empty() || !step.isFinal()) /// is not final need Singleton
        {
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
        }
        else
        {
            /// TODO Hashed by bucket
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});
        }

        res.emplace_back(required_child_prop);
        return res;
    }

    AlternativeChildrenProp visit(ExpressionStep & /*step*/) override
    {
        AlternativeChildrenProp res;
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
        return res;
    }

    AlternativeChildrenProp visit(SortingStep & /*step*/) override
    {
        AlternativeChildrenProp res;
        //    const auto & sort_description = step.getSortDescription();
        //    PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = sort_description};

        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
        return res;
    }


    AlternativeChildrenProp visit(LimitStep & step) override
    {
        AlternativeChildrenProp res;

        std::vector<PhysicalProperties> required_child_prop;

        if (step.getStepDescription().contains("preliminary"))
        {
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        }
        else
        {
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
        }

        res.emplace_back(required_child_prop);
        return res;
    }

    AlternativeChildrenProp visit(JoinStep & /*step*/) override
    {
        AlternativeChildrenProp res;

        /// broadcast join

        AlternativeChildrenProp alternative_properties;

        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});

        res.emplace_back(required_child_prop);

        /// shaffle join
        AlternativeChildrenProp alternative_properties1;
        std::vector<PhysicalProperties> required_child_prop1;
        required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});
        required_child_prop1.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed}});

        res.emplace_back(required_child_prop1);

        return res;
    }

    AlternativeChildrenProp visit(ExchangeDataStep & step) override
    {
        AlternativeChildrenProp res;

        std::vector<PhysicalProperties> required_child_prop;
        if (step.hasSortInfo())
        {
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_description = step.getSortDescription()});
        }
        else
        {
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        }

        res.emplace_back(required_child_prop);
        return res;
    }

private:
    GroupNode & group_node;
};

}
