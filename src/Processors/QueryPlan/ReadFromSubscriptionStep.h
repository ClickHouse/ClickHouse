#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

#include <Storages/Streaming/Subscription.h>

namespace DB
{

class ReadFromSubscriptionStep final : public ISourceStep
{
public:
    ReadFromSubscriptionStep(Block storage_sample_, Block desired_header_, StreamSubscriptionPtr subscription_);

    String getName() const override { return "ReadFromSubscription"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    Block storage_sample;
    Block desired_header;

    StreamSubscriptionPtr subscription;
};

}
