#pragma once

#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/Statistics/Statistics.h>
#include <QueryCoordination/NewOptimizer/PlanStepVisitor.h>

namespace DB
{

class CostCalculator : public PlanStepVisitor<Float64>
{
public:
    using Base = PlanStepVisitor<Float64>;

    CostCalculator(const PhysicalProperties & prop_, const Statistics & statistics_, const std::vector<Statistics> & input_statistics_)
        : prop(prop_), statistics(statistics_), input_statistics(input_statistics_)
    {
    }

    Float64 visit(QueryPlanStepPtr step) override
    {
        return Base::visit(step);
    }

    Float64 visit(ReadFromMergeTree & /*step*/) override
    {
        /// TODO get rows by statistics
        return 3 * statistics.getOutputRowSize();
    }

    Float64 visitStep() override
    {
        return 3 * input_statistics.front().getOutputRowSize();
    }

    Float64 visit(AggregatingStep & step) override
    {
        if (step.isFinal())
        {
            /// TODO get rows, cardinality by statistics
            if (prop.distribution.type == PhysicalProperties::DistributionType::Hashed)
            {
                return 6 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/); /// fake shard_num
            }
            else
                return 6 * (input_statistics.front().getOutputRowSize());
        }
        else
        {
            /// TODO get rows, cardinality by statistics
            return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
        }
    }

    Float64 visit(MergingAggregatedStep & /*step*/) override
    {
        /// TODO get rows, cardinality by statistics
        if (prop.distribution.type == PhysicalProperties::DistributionType::Hashed)
        {
            return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
        }
        return 3 * input_statistics.front().getOutputRowSize();
    }

    Float64 visit(ExchangeDataStep & step) override
    {
        /// TODO get rows, cardinality by statistics
        /// TODO by type
        if (step.getDistributionType() == PhysicalProperties::DistributionType::Replicated)
        {
            return 2 * (statistics.getOutputRowSize() * 3/*shard_num*/);
        }
        return 2 * statistics.getOutputRowSize();
    }

    Float64 visit(SortingStep & /*step*/) override
    {
        if (prop.distribution.type == PhysicalProperties::DistributionType::Singleton)
        {
            return 3 * input_statistics.front().getOutputRowSize();
        }
        return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
    }

    Float64 visit(JoinStep & /*step*/) override
    {
        /// TODO Hash join memory cost low

        return 4 * (Float64(statistics.getOutputRowSize()) / 3/*shard_num*/);
    }

    Float64 visit(LimitStep & step) override
    {
        if (step.getStepDescription().contains("preliminary LIMIT"))
        {
            if (prop.distribution.type == PhysicalProperties::DistributionType::Singleton)
            {
                return 1 * (input_statistics.front().getOutputRowSize() + 1); /// +1 singleton don't need preliminary
            }
            return 1 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
        }
        else if (step.getStepDescription().contains("final LIMIT"))
        {
            return 1 * input_statistics.front().getOutputRowSize();
        }
        else
        {
            return 1 * input_statistics.front().getOutputRowSize();
        }
    }

private:
    const PhysicalProperties & prop;
    const Statistics & statistics;
    const std::vector<Statistics> & input_statistics;
};

}
