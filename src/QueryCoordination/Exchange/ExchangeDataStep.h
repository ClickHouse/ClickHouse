#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{

public:
    struct SortInfo
    {
        size_t max_block_size;
        bool always_read_till_end = false;
        UInt64 limit;
        SortDescription result_description;
    };


    ExchangeDataStep(Int32 fragment_id_, const DataStream & data_stream, StorageLimitsList & storage_limits_)
        : ISourceStep(data_stream), fragment_id(fragment_id_), storage_limits(std::make_shared<StorageLimitsList>(storage_limits_))
    {
    }

    ExchangeDataStep(PhysicalProperties::Distribution distribution_, const DataStream & data_stream)
        : ISourceStep(data_stream), distribution(distribution_)
    {
        setStepDescription(PhysicalProperties::distributionType(distribution.type));
    }

    String getName() const override { return "ExchangeData"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;

    void setPlanID(UInt32 plan_id_)
    {
        plan_id = plan_id_;
    }

    void setSources(const std::vector<String> & sources_)
    {
        sources = sources_;
    }

    void setSortInfo(const SortInfo & sort_info_)
    {
        sort_info = sort_info_;
        has_sort_info = true;
    }

    bool hasSortInfo() const
    {
        return has_sort_info;
    }

    SortDescription getSortDescription() const
    {
        return sort_info.result_description;
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
    void mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_);

    UInt32 fragment_id;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    std::vector<String> sources;

    UInt32 plan_id;

    bool has_sort_info = false;

    SortInfo sort_info;

    PhysicalProperties::Distribution distribution;
};

}
