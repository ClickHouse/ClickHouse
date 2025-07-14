#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

/// Receive part of BroadcastExchangeStep
class BroadcastReceiveStep : public ISourceStep
{
public:
    BroadcastReceiveStep(SharedHeader header_, const String & exchange_id_, const Strings & source_shards_)
        : ISourceStep(std::move(header_))
        , exchange_id(exchange_id_)
        , source_shards(source_shards_)
    {
        /// TODO: is there a scenario where we broadcast partitioned source and thus have multiple source shards?
        chassert(source_shards.size() == 1);
    }

    String getName() const override { return "BroadcastReceive"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    const String exchange_id;
    const Strings source_shards;
};

}
