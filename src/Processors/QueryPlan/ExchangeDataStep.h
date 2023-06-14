#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{

public:
    ExchangeDataStep(const DataStream & data_stream, std::shared_ptr<const StorageLimitsList> storage_limits_)
        : ISourceStep(data_stream), storage_limits(std::move(storage_limits_))
    {
    }

    String getName() const override { return "ExchangeData"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;

    void setPlanID(Int32 plan_id_)
    {
        plan_id = plan_id_;
    }

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;

    Int32 plan_id;
};

}
