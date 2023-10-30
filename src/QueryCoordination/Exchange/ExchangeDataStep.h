#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{

public:
    ExchangeDataStep(
        PhysicalProperties::Distribution distribution_,
        const DataStream & data_stream,
        size_t max_block_size_,
        SortDescription sort_description_ = {},
        DataStream::SortScope sort_scope_ = DataStream::SortScope::None,
        bool exchange_sink_merge = false,
        bool exchange_source_merge = false)
        : ISourceStep(data_stream)
        , distribution(distribution_)
        , max_block_size(max_block_size_)
        , sort_description(sort_description_)
        , sink_merge(exchange_sink_merge)
        , source_merge(exchange_source_merge)
    {
        setStepDescription(PhysicalProperties::distributionType(distribution.type));

        output_stream->sort_description = sort_description;
        output_stream->sort_scope = sort_scope_;
    }

    String getName() const override { return "ExchangeData"; }

    StepType stepType() const override { return Exchange; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;

    void mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_);

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

    const SortDescription & getSortDescription() const
    {
        return sort_description;
    }

    bool sinkMerge() const
    {
        return sink_merge;
    }

private:
    UInt32 fragment_id;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    std::vector<String> sources;

    UInt32 plan_id;

    PhysicalProperties::Distribution distribution;

    size_t max_block_size;

    SortDescription sort_description;

    bool sink_merge;

    bool source_merge;
};

}
