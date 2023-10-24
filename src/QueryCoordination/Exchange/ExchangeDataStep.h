#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{

public:
    ExchangeDataStep(PhysicalProperties::Distribution distribution_, const DataStream & data_stream)
        : ISourceStep(data_stream), distribution(distribution_)
    {
        setStepDescription(PhysicalProperties::distributionType(distribution.type));
    }

    String getName() const override { return "ExchangeData"; }

    StepType stepType() const override { return Exchange; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;

    void setPlanID(UInt32 plan_id_)
    {
        plan_id = plan_id_;
    }

    void setSources(const std::vector<String> & sources_)
    {
        sources = sources_;
    }

    PhysicalProperties::DistributionType getDistributionType() const
    {
        return distribution.type;
    }

    bool isSingleton() const
    {
        return distribution.type == PhysicalProperties::DistributionType::Singleton;
    }

    const PhysicalProperties::Distribution & getDistribution() const
    {
        return distribution;
    }

    void setFragmentId(UInt32 fragment_id_)
    {
        fragment_id = fragment_id_;
    }

private:
    UInt32 fragment_id;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    std::vector<String> sources;

    UInt32 plan_id;

    PhysicalProperties::Distribution distribution;
};

}
