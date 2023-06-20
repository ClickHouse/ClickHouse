#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{

public:
    ExchangeDataStep(const DataStream & data_stream, StorageLimitsList & storage_limits_)
        : ISourceStep(data_stream), storage_limits(std::make_shared<StorageLimitsList>(storage_limits_))
    {
    }

    String getName() const override { return "ExchangeData"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;

    void setPlanID(Int32 plan_id_)
    {
        plan_id = plan_id_;
    }

    void setSources(const std::vector<String> & sources_)
    {
        sources = sources_;
    }

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;

    std::vector<String> sources;

    Int32 plan_id;
};

}
